using System.Text.Json.Nodes;
using LawnDart.EventStore;

namespace LawnDart.Backends.Contract.Tests;

public abstract class ContractSuite
{
    protected ContractSuite(IContractBackend backend) => Backend = backend;

    protected IContractBackend Backend { get; }

    protected ContractCapabilities Cap => Backend.Capabilities;

    protected static void SkipUnless(bool available, string reason) => ContractSkip.Unless(available, reason);

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

    protected static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token).ConfigureAwait(false);
    }
}
