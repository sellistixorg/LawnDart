using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;

namespace LawnDart.Backends.Contract.Tests;

[Trait("Category", "Contract")]
public sealed class InMemoryEventLogContractTests
{
    private readonly IEventLog _log = new InMemoryEventStore();

    [Fact]
    public Task Thesis_HostWithoutClrTypes_CanReadFilterAndCopy()
        => EventLogContract.Thesis_HostWithoutClrTypes_CanReadFilterAndCopyAsync(_log);

    [Fact]
    public Task AppendRead_SamePayloadWithoutHydrate()
        => EventLogContract.AppendRead_SamePayloadWithoutHydrateAsync(_log);

    [Fact]
    public Task Query_TokenAndTags_WithoutHydrate()
        => EventLogContract.Query_TokenAndTags_WithoutHydrateAsync(_log);

    [Fact]
    public Task TypedAdapter_RegisteredType_StillRoundTrips()
        => EventLogContract.TypedAdapter_RegisteredType_StillRoundTripsAsync(_log);

    [Fact]
    public Task TypedAdapter_UnknownFamily_FailsClosed()
        => EventLogContract.TypedAdapter_UnknownFamily_FailsClosedAsync(_log);

    [Fact]
    public Task TypedAdapter_ContentTypeMismatch_FailsClosed()
        => EventLogContract.TypedAdapter_ContentTypeMismatch_FailsClosedAsync(_log);
}
