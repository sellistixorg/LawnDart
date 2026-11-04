using LawnDart.EventStore;
using LawnDart.Outbox;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;
using LawnDart.Snapshots;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// A LawnDart backend with every store the contract tests cover: the event store from
/// the package, plus outbox, inbox, snapshot and projection stores.
/// </summary>
public interface IFullContractBackend : IContractBackend
{
    Task<IOutboxWriter> OpenOutboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<ContractInbox> OpenInboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<ContractSnapshots> OpenSnapshotsAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<ContractProjectionStores> OpenProjectionStoresAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);
}

/// <summary>
/// Base class for the suites that stay in this project because they need more than an event store.
/// </summary>
public abstract class BackendContractSuite : ContractSuite
{
    protected BackendContractSuite(IFullContractBackend backend)
        : base(backend)
        => Backend = backend;

    protected new IFullContractBackend Backend { get; }
}

public sealed class ContractInbox
{
    public required LawnDart.Messaging.IInboxStore Store { get; init; }

    public Func<CancellationToken, Task<int>>? PurgeExpiredAsync { get; init; }

    public Func<CancellationToken, Task<int>>? CountRowsAsync { get; init; }

    public Func<CancellationToken, Task<string?>>? ReadSingleKeyAsync { get; init; }

    public Func<CancellationToken, Task<LawnDart.Messaging.IInboxStore>>? OpenSecondInstanceAsync { get; init; }
}

public sealed class ContractSnapshots
{
    public required IEventStore Store { get; init; }

    public required ISnapshotStore Snapshots { get; init; }

    public required IDcbSnapshotStore DcbSnapshots { get; init; }

    public Func<string, CancellationToken, Task<int>>? CorruptDcbSnapshotAsync { get; init; }

    public Func<string, CancellationToken, Task<IReadOnlyList<string>?>>? ReadDcbLoadTagsAsync { get; init; }
}

public sealed class ContractProjectionStores
{
    public required IViewStore Views { get; init; }

    public required ICheckpointStore Checkpoints { get; init; }
}

public static class ContractEventStoreExtensions
{
    /// <summary>The outbox the backend opened with the store. Every LawnDart backend opens one.</summary>
    public static IOutboxWriter RequireOutbox(this ContractEventStore opened)
        => opened.Outbox ?? throw new InvalidOperationException("This backend did not open an outbox with the event store.");
}
