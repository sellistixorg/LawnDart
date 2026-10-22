using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.Aggregates;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Postgres;
using LawnDart.Projections.Storage;
using LawnDart.Testing;
using Library.Domain;
using Library.Host;
using Library.Projections;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Proves <see cref="LibraryHost.AddPostgresLibrary"/> materialises the catalog view in Postgres.
/// Schema setup is <see cref="LibraryHost.InitializePostgresLibraryAsync"/>, called here as the
/// deploy step. The host registration does not create tables.
/// </summary>
[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresLibraryHostTests(PostgresContractFixture postgres)
{
    [Fact]
    public async Task AddPostgresLibrary_borrow_materialises_catalog_view()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        LibraryHost.AddPostgresLibrary(services, postgres.AdminConnectionString);

        await using var provider = services.BuildServiceProvider();
        await LibraryHost.InitializePostgresLibraryAsync(provider);

        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);

        try
        {
            var bookId = Guid.NewGuid();
            var add = provider.GetRequiredService<ICommandHandler<AddBookCommand>>();
            var borrow = provider.GetRequiredService<ICommandHandler<BorrowBookCommand>>();

            await add.HandleAsync(new AddBookCommand(Guid.NewGuid(), bookId, "A Tale of Two Cities", "ABCD"));
            await borrow.HandleAsync(new BorrowBookCommand(Guid.NewGuid(), bookId, "Jeremy"));

            var views = provider.GetRequiredKeyedService<IViewStore>("default");
            Assert.IsType<PostgresProjectionStore>(views);

            var streamId = $"Book:{bookId}";
            string? viewJson = null;
            await WaitForAsync.UntilAsync(async () =>
            {
                viewJson = await views.GetViewAsync("BookCatalog:v1", streamId);
                return viewJson is not null && viewJson.Contains("Jeremy", StringComparison.Ordinal);
            }, timeout: TimeSpan.FromSeconds(20));

            var view = JsonSerializer.Deserialize<BookCatalogView>(viewJson!, ProjectionViewJson.Read);
            Assert.NotNull(view);
            Assert.Equal("A Tale of Two Cities", view.Title);
            Assert.Equal("Jeremy", view.BorrowedBy);
            Assert.True(view.OnLoan);
        }
        finally
        {
            foreach (var service in hosted.Reverse())
                await service.StopAsync(CancellationToken.None);
        }
    }
}
