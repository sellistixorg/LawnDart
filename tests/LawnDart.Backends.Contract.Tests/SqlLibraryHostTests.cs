using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Projections.Storage;
using LawnDart.Testing;
using Library.Domain;
using Library.Host;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Proves <see cref="LibraryHost.AddSqlLibrary"/> materialises the catalog view in SQL Server.
/// </summary>
[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
public sealed class SqlLibraryHostTests(SqlServerContractFixture sql)
{
    [Fact]
    public async Task AddSqlLibrary_borrow_materialises_catalog_view()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        LibraryHost.AddSqlLibrary(services, sql.ConnectionString);

        await using var provider = services.BuildServiceProvider();
        await LibraryHost.InitializeSqlLibraryAsync(provider);

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
            Assert.IsType<SqlViewStore>(views);

            var streamId = $"Book:{bookId}";
            string? viewJson = null;
            await WaitForAsync.UntilAsync(async () =>
            {
                viewJson = await views.GetViewAsync("BookCatalog:v1", streamId);
                return viewJson is not null
                    && viewJson.Contains("Jeremy", StringComparison.Ordinal)
                    && viewJson.Contains("\"onLoan\":true", StringComparison.Ordinal);
            }, timeout: TimeSpan.FromSeconds(20));

            Assert.Contains("A Tale of Two Cities", viewJson, StringComparison.Ordinal);
        }
        finally
        {
            foreach (var service in hosted.Reverse())
                await service.StopAsync(CancellationToken.None);
        }
    }
}
