using LawnDart.AspNetCore;
using LawnDart.Authorization.AspNetCore;
using LawnDart.Projections.Lightweight;
using Library.Domain;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Library.Host;

/// <summary>
/// Runs the Library host on Postgres. Schema setup is the <c>setup</c> command.
/// Starting the app does not create tables.
/// </summary>
public static class Program
{
    /// <summary>
    /// Local Postgres started by <c>samples/Library.Host/docker-compose.yml</c>.
    /// </summary>
    public const string DefaultConnectionString =
        "Host=localhost;Port=5432;Database=library;Username=library;Password=library";

    /// <summary>
    /// <c>setup</c> creates the tables and exits. Any other argument list starts the app.
    /// </summary>
    /// <param name="args">Command line arguments.</param>
    public static async Task<int> Main(string[] args)
    {
        if (args is ["setup"])
        {
            await SetupAsync().ConfigureAwait(false);
            return 0;
        }

        await RunAsync(args).ConfigureAwait(false);
        return 0;
    }

    /// <summary>
    /// Connection string from <c>ConnectionStrings:Library</c>, then
    /// <c>LAWNDART_LIBRARY_POSTGRES</c>, then <see cref="DefaultConnectionString"/>.
    /// </summary>
    /// <param name="configuration">Optional configuration. Environment variables still apply.</param>
    public static string ResolveConnectionString(IConfiguration? configuration = null)
    {
        var configured = configuration?["ConnectionStrings:Library"];
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        var env = Environment.GetEnvironmentVariable("LAWNDART_LIBRARY_POSTGRES");
        if (!string.IsNullOrWhiteSpace(env))
            return env;

        return DefaultConnectionString;
    }

    private static async Task SetupAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));
        LibraryHost.AddPostgresLibrary(services, ResolveConnectionString());
        await using var provider = services.BuildServiceProvider();
        await LibraryHost.InitializePostgresLibraryAsync(provider).ConfigureAwait(false);
    }

    private static async Task RunAsync(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var connectionString = ResolveConnectionString(builder.Configuration);
        LibraryHost.AddPostgresLibrary(builder.Services, connectionString);
        builder.Services.AddLawnDartHttpCommands(typeof(BorrowBookHandler).Assembly);
        builder.Services.AddHttpAuthorizationContext();

        var urls = Environment.GetEnvironmentVariable("ASPNETCORE_URLS");
        if (string.IsNullOrWhiteSpace(urls))
            builder.WebHost.UseUrls("http://127.0.0.1:5088");

        var app = builder.Build();
        app.MapLawnDartCommands();
        app.MapProjectionQueries("default");
        await app.RunAsync().ConfigureAwait(false);
    }
}
