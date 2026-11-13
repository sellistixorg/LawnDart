using LawnDart.EventStore;
using LawnDart.Publishing;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Turns <see cref="ContractStoreOptions.PublishedEvents"/> into the map a store appends with.
/// Outbox on and nothing declared fails with the same message as <c>UseSqlServer</c>,
/// <c>UsePostgres</c>, and <c>UseInMemory</c>.
/// </summary>
internal static class ContractOutboxMaps
{
    public static PublishedEventMap? For(ContractStoreOptions options)
    {
        if (options.EnableOutbox && options.PublishedEvents is null)
        {
            EventTypeCatalog? catalog = options.EventTypes is { Count: > 0 }
                ? EventTypeCatalog.Materialize(options.EventTypes)
                : null;
            var name = string.IsNullOrWhiteSpace(options.SchemaName) ? "default" : options.SchemaName;
            throw new InvalidOperationException(
                BoundedContextExtensions.MissingPublishedEventsMessage(name, catalog));
        }

        return options.CreatePublishedMap();
    }
}
