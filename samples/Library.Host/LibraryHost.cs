using LawnDart;
using LawnDart.AspNetCore;
using LawnDart.Authorization.AspNetCore;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using LawnDart.Messaging.Postgres;
using LawnDart.Messaging.SqlServer;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Postgres;
using LawnDart.Projections.Lightweight.SqlServer;
using Library.Domain;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Library.Host;

public static class LibraryHost
{
    public static IServiceCollection AddInMemoryLibrary(IServiceCollection services)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddInMemoryMessaging();
        services.AddReactor<LoanNoticeReactor, BookBorrowed>();
        services.AddInMemoryProjectionStores("default");
        var ctx = services.AddBoundedContext("default");
        ctx.UseInMemory();
        ctx.WithCommandHandlers<BorrowBookHandler>();
        ctx.WithEventTypes<BookAdded>();
        ctx.WithProjections(
            [typeof(LibraryCatalogProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 100;
            });
        return services;
    }

    /// <summary>
    /// Registers the Library host on SQL Server: event store, projection stores,
    /// in-memory transport, SQL inbox, loan-notice reactor, and transactional outbox.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <returns>The same service collection.</returns>
    public static IServiceCollection AddSqlLibrary(IServiceCollection services, string connectionString)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddInMemoryMessaging();
        services.AddSqlInboxStore(connectionString);
        services.AddMessageTransportOutboxPublisher();
        services.AddReactor<LoanNoticeReactor, BookBorrowed>();
        services.AddSqlProjectionStores("default", connectionString);
        var ctx = services.AddBoundedContext("default");
        ctx.UseSqlServer(o =>
        {
            o.ConnectionString = connectionString;
            o.RequireTenantId = false;
            o.EnableOutbox = true;
        });
        ctx.WithCommandHandlers<BorrowBookHandler>();
        ctx.WithEventTypes<BookAdded>();
        ctx.WithProjections(
            [typeof(LibraryCatalogProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 100;
            });
        return services;
    }

    /// <summary>
    /// Creates event-store, outbox, projection, and inbox tables for <see cref="AddSqlLibrary"/>.
    /// Call this after the provider is built and before the first command.
    /// </summary>
    /// <param name="services">The built provider from <see cref="AddSqlLibrary"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task InitializeSqlLibraryAsync(
        IServiceProvider services,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(services);

        var store = services.GetRequiredKeyedService<IEventStore>("default");
        if (store is not SqlServerEventStore sql)
        {
            throw new InvalidOperationException(
                "AddSqlLibrary did not resolve SqlServerEventStore for context 'default'.");
        }

        await sql.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        await services.GetRequiredKeyedService<IOutboxWriter>("default")
            .InitializeSchemaAsync(cancellationToken)
            .ConfigureAwait(false);
        await services.InitializeSqlProjectionStoresAsync("default", cancellationToken).ConfigureAwait(false);
        await services.InitializeSqlInboxStoreAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Registers the Library host on Postgres: event store, projection stores,
    /// in-memory transport, Postgres inbox, loan-notice reactor, and transactional outbox.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">Postgres connection string for reads and appends.</param>
    /// <returns>The same service collection.</returns>
    public static IServiceCollection AddPostgresLibrary(IServiceCollection services, string connectionString)
    {
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddInMemoryMessaging();
        services.AddPostgresInboxStore(connectionString);
        services.AddMessageTransportOutboxPublisher();
        services.AddReactor<LoanNoticeReactor, BookBorrowed>();
        services.AddPostgresProjectionStores("default", connectionString);
        var ctx = services.AddBoundedContext("default");
        ctx.UsePostgres(o =>
        {
            o.ConnectionString = connectionString;
            o.RequireTenantId = false;
            o.EnableOutbox = true;
        });
        ctx.WithCommandHandlers<BorrowBookHandler>();
        ctx.WithEventTypes<BookAdded>();
        ctx.WithProjections(
            [typeof(LibraryCatalogProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 100;
            });
        return services;
    }

    /// <summary>
    /// Creates event-store, outbox, projection, and inbox tables for <see cref="AddPostgresLibrary"/>.
    /// Call this from a deploy or admin step, after the provider is built and before the first command.
    /// The running app does not call it.
    /// </summary>
    /// <param name="services">The built provider from <see cref="AddPostgresLibrary"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task InitializePostgresLibraryAsync(
        IServiceProvider services,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(services);

        var store = services.GetRequiredKeyedService<IEventStore>("default");
        if (store is not PostgresEventStore postgres)
        {
            throw new InvalidOperationException(
                "AddPostgresLibrary did not resolve PostgresEventStore for context 'default'.");
        }

        await postgres.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        await services.GetRequiredKeyedService<IOutboxWriter>("default")
            .InitializeSchemaAsync(cancellationToken)
            .ConfigureAwait(false);
        await services.InitializePostgresProjectionStoresAsync("default", cancellationToken).ConfigureAwait(false);
        await services.InitializePostgresInboxStoreAsync(cancellationToken).ConfigureAwait(false);
    }

    public static void AddTwoContexts(IServiceCollection services)
    {
        var library = services.AddBoundedContext("library");
        library.UseInMemory();
        library.WithCommandHandlers<BorrowBookHandler>();
        library.WithEventTypes<BookAdded>();

        var other = services.AddBoundedContext("other");
        other.UseInMemory();
        other.WithEventTypes<BookAdded>();
    }

    public static void MapLibraryHttp(IServiceCollection services, WebApplication app)
    {
        services.AddLawnDartHttpCommands(typeof(BorrowBookHandler).Assembly);
        services.AddHttpAuthorizationContext();
        app.MapLawnDartCommands();
        app.MapProjectionQueries("default");
    }
}
