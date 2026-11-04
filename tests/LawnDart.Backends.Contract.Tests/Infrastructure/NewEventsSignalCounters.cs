using LawnDart.EventSourcing.Testing;
using LawnDart.EventStore;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>Reads the new-events signal counters that LawnDart's own stores keep internally.</summary>
internal sealed class NewEventsSignalCounters(NewEventsSignal signal) : IContractSignalCounters
{
    public long NotifyCount => signal.Generation;

    public int WaiterCount => signal.WaitingCount;
}
