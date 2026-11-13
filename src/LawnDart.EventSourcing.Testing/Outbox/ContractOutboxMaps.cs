using LawnDart.EventStore;
using LawnDart.Publishing;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Turns <see cref="ContractStoreOptions.PublishedEvents"/> into the map a store appends with.
/// Outbox on and nothing declared fails with the same message as <c>UseSqlServer</c>,
/// <c>UsePostgres</c>, and <c>UseInMemory</c>.
/// </summary>
public static class ContractOutboxMaps
{
    /// <summary>
    /// The published-event map for <paramref name="options"/>, or <c>null</c> when the outbox is off.
    /// </summary>
    /// <param name="options">The options for this contract store.</param>
    /// <exception cref="InvalidOperationException">
    /// The outbox is on and <see cref="ContractStoreOptions.PublishedEvents"/> is null.
    /// The message is <see cref="PublishedEventMap.NothingDeclaredMessage"/>.
    /// </exception>
    public static PublishedEventMap? For(ContractStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.EnableOutbox && options.PublishedEvents is null)
        {
            EventTypeCatalog? catalog = options.EventTypes is { Count: > 0 }
                ? EventTypeCatalog.Materialize(options.EventTypes)
                : null;
            var name = string.IsNullOrWhiteSpace(options.SchemaName) ? "default" : options.SchemaName;
            throw new InvalidOperationException(
                PublishedEventMap.NothingDeclaredMessage(name, catalog));
        }

        return options.CreatePublishedMap();
    }
}
