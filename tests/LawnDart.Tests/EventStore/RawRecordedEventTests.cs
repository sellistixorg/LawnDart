using System.Text;
using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class RawRecordedEventTests
{
    [Fact]
    public void From_copies_token_and_payload_and_does_not_mint_identity()
    {
        var payload = Encoding.UTF8.GetBytes("""{"kind":"opaque"}""");
        var recorded = new RecordedEvent(
            "foreign-family",
            payload,
            "s",
            streamVersion: 1,
            sequencePosition: 7,
            commitTimestamp: new DateTime(2026, 9, 16, 12, 0, 0, DateTimeKind.Utc));

        var raw = RawRecordedEvent.From(recorded);

        Assert.Same(recorded, raw.Recorded);
        Assert.Equal("foreign-family", raw.TypeName);
        Assert.Equal(payload, raw.RawPayload);
        Assert.Equal(Guid.Empty, raw.Id);
        Assert.Equal(default, raw.Timestamp);
    }

    [Fact]
    public void From_reads_event_id_and_timestamp_from_metadata_json()
    {
        var id = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var timestamp = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var metadata = Encoding.UTF8.GetBytes(
            $$"""{"EventId":"{{id}}","Timestamp":"{{timestamp:O}}"}""");
        var recorded = new RecordedEvent(
            "named.tick",
            Encoding.UTF8.GetBytes("{}"),
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: new DateTime(2026, 9, 16, 0, 0, 0, DateTimeKind.Utc),
            metadata);

        var raw = RawRecordedEvent.From(recorded);

        Assert.Equal(id, raw.Id);
        Assert.Equal(timestamp, raw.Timestamp);
        Assert.NotEqual(recorded.CommitTimestamp, raw.Timestamp);
    }

    [Fact]
    public void Mutating_raw_payload_does_not_change_recorded_frame()
    {
        var recorded = new RecordedEvent(
            "named.tick",
            new byte[] { 1, 2, 3 },
            "s",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow);

        var raw = RawRecordedEvent.From(recorded);
        raw.RawPayload[0] = 99;

        Assert.Equal(1, recorded.Payload.Span[0]);
        Assert.Equal(99, raw.RawPayload[0]);
    }
}
