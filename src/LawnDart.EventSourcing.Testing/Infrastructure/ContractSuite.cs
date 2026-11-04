using System.Text.Json.Nodes;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Base class for every contract suite. A store's test project inherits a suite,
/// passes its <see cref="IContractBackend"/> to the constructor, and xUnit runs the tests.
/// </summary>
public abstract class ContractSuite
{
    protected ContractSuite(IContractBackend backend) => Backend = backend;

    protected IContractBackend Backend { get; }

    protected ContractCapabilities Cap => Backend.Capabilities;

    protected static void SkipUnless(bool available, string reason) => ContractSkip.Unless(available, reason);

    /// <summary>The store's live subscriptions, or a skip when the backend has none.</summary>
    protected static IEventStoreSubscriptions SubscriptionsOf(ContractEventStore opened)
    {
        SkipUnless(opened.Subscriptions is not null, "This backend has no live subscriptions.");
        return opened.Subscriptions!;
    }

    /// <summary>The store's new-events signal, or a skip when the backend has none.</summary>
    protected static ISignalsNewEvents SignalsOf(ContractEventStore opened)
    {
        SkipUnless(opened.Signals is not null, "This backend has no new-events signal.");
        return opened.Signals!;
    }

    protected static async Task<List<SequencedEvent>> ReadExactlyAsync(
        ISubscriptionHandle handle,
        int count,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var list = new List<SequencedEvent>(count);
        while (list.Count < count)
        {
            if (!await handle.Events.WaitToReadAsync(cts.Token).ConfigureAwait(false))
                throw new TimeoutException($"Channel completed after {list.Count} of {count} events.");
            while (list.Count < count && handle.Events.TryRead(out var sequenced))
                list.Add(sequenced);
        }

        return list;
    }

    protected static async Task<List<RecordedEvent>> ReadRecordedAsync(
        IEventLogSubscriptionHandle handle,
        int count,
        TimeSpan timeout)
    {
        var list = new List<RecordedEvent>(count);
        using var cts = new CancellationTokenSource(timeout);
        while (list.Count < count && await handle.Events.WaitToReadAsync(cts.Token).ConfigureAwait(false))
        {
            while (list.Count < count && handle.Events.TryRead(out var recorded))
                list.Add(recorded);
        }

        if (list.Count < count)
            throw new TimeoutException($"Expected {count} events, received {list.Count}.");
        return list;
    }

    protected static async Task<List<SequencedEvent>> DrainAsync(ISubscriptionHandle handle, TimeSpan wait)
    {
        var list = new List<SequencedEvent>();
        using var cts = new CancellationTokenSource(wait);
        try
        {
            while (await handle.Events.WaitToReadAsync(cts.Token).ConfigureAwait(false))
            {
                while (handle.Events.TryRead(out var sequenced))
                    list.Add(sequenced);
            }
        }
        catch (OperationCanceledException)
        {
            // The wait ended.
        }

        return list;
    }

    protected static async Task<List<RecordedEvent>> DrainRecordedAsync(IEventLogSubscriptionHandle handle, TimeSpan wait)
    {
        var list = new List<RecordedEvent>();
        using var cts = new CancellationTokenSource(wait);
        try
        {
            while (await handle.Events.WaitToReadAsync(cts.Token).ConfigureAwait(false))
            {
                while (handle.Events.TryRead(out var recorded))
                    list.Add(recorded);
            }
        }
        catch (OperationCanceledException)
        {
            // The wait ended.
        }

        return list;
    }

    protected void AssertStoredPayload(byte[] expected, ReadOnlyMemory<byte> actual, byte codecId)
    {
        var bytes = actual.ToArray();
        if (Cap.PreservesExactJsonText || codecId != EventCodec.Json)
        {
            Assert.Equal(expected, bytes);
            return;
        }

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(bytes)));
    }

    protected void AssertStoredMetadata(byte[] expected, ReadOnlyMemory<byte> actual)
    {
        if (expected.Length == 0)
        {
            Assert.True(actual.IsEmpty);
            return;
        }

        var bytes = actual.ToArray();
        if (Cap.PreservesExactJsonText)
        {
            Assert.Equal(expected, bytes);
            return;
        }

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(bytes)));
    }

    protected void AssertViewJson(string expected, string? actual)
    {
        Assert.NotNull(actual);
        if (Cap.PreservesExactViewJson)
        {
            Assert.Equal(expected, actual);
            return;
        }

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(actual)));
    }

    /// <summary>The store's signal counters, or a skip when the backend has none.</summary>
    protected static IContractSignalCounters SignalCountersOf(ContractEventStore opened)
    {
        SkipUnless(opened.SignalCounters is not null, "This backend does not supply signal counters.");
        return opened.SignalCounters!;
    }

    protected static async Task WaitForWaitersAsync(IContractSignalCounters counters, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (counters.WaiterCount < count)
            await Task.Delay(10, cts.Token).ConfigureAwait(false);
    }
}
