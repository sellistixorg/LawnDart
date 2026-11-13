using System.Text;
using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Publishing;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Outbox rows follow the context's published events. A backend that writes
/// outbox rows on append runs these tests.
/// </summary>
public abstract class OutboxPublicationContractTests : ContractSuite
{
    private static readonly JsonSerializerOptions MetadataJson = new()
    {
        PropertyNameCaseInsensitive = true
    };

    protected OutboxPublicationContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public async Task StreamAppend_WritesRowsOnlyForDeclaredEvents()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var stream = Stream();
        var pass = Pass("through");
        var translated = Translated("moved", "secret-note");
        var hidden = Internal("hidden");
        var skipped = Skipped();
        var result = await opened.Store.AppendAsync(stream, [pass, translated, hidden, skipped]);

        Assert.Equal(4, (await opened.Store.ReadStreamAsync(stream)).Count);
        var rows = await Rows(opened);
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, row => row.Id == pass.Id && row.EventType == "backend-contract.outbox-event");
        Assert.Contains(rows, row => row.Id == translated.Id && row.EventType == "backend-contract.outbox-notice");
        Assert.DoesNotContain(rows, row => row.EventType == "backend-contract.internal-only");
        Assert.DoesNotContain(rows, row => row.EventType == "backend-contract.skipped-source");
        Assert.DoesNotContain(rows, row => row.EventType == "backend-contract.translated-source");
        Assert.Equal(result.SequencePositions[0], rows.Single(row => row.Id == pass.Id).SequencePosition);
        Assert.Equal(result.SequencePositions[1], rows.Single(row => row.Id == translated.Id).SequencePosition);
    }

    [ContractFact]
    public async Task DcbAppend_WritesRowsOnlyForDeclaredEvents()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var tag = Tag();
        var pass = Pass("dcb-through");
        var translated = Translated("dcb-moved", "dcb-secret");
        var result = await opened.Store.AppendAsync(
            [
                new TaggedEvent(pass, [tag]),
                new TaggedEvent(translated, [tag]),
                new TaggedEvent(Internal("dcb-hidden"), [tag]),
                new TaggedEvent(Skipped(), [tag])
            ],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));

        var rows = await Rows(opened);
        Assert.Equal(2, rows.Count);
        Assert.Equal(result.SequencePositions[0], rows.Single(row => row.Id == pass.Id).SequencePosition);
        Assert.Equal(result.SequencePositions[1], rows.Single(row => row.Id == translated.Id).SequencePosition);
        Assert.DoesNotContain(rows, row => row.EventType == "backend-contract.internal-only");
        Assert.DoesNotContain(rows, row => row.EventType == "backend-contract.skipped-source");
    }

    [ContractFact]
    public async Task Translation_RowIsTheNotice()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var stream = Stream();
        var source = Translated("public-value", "internal-only-note");
        await opened.Store.AppendAsync(stream, [source]);

        var stored = Assert.Single(await opened.Log.ReadStreamAsync(stream));
        var row = Assert.Single(await Rows(opened));
        Assert.Equal("backend-contract.translated-source", stored.EventType);
        Assert.Equal(1, stored.SchemaVersion);
        Assert.Equal("backend-contract.outbox-notice", row.EventType);
        Assert.Equal(2, row.SchemaVersion);
        Assert.Equal(stored.CodecId, row.CodecId);
        var payload = Encoding.UTF8.GetString(row.Payload.Span);
        Assert.Contains("public-value", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("internal-only-note", payload, StringComparison.Ordinal);
    }

    [ContractFact]
    public async Task RowId_IsTheSourceEventId()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var source = Translated("id-check", "keep-off-the-row");
        await opened.Store.AppendAsync(
            Stream(),
            [source],
            metadata: new EventMetadata { CorrelationId = "corr-9", CausationId = "cause-9", TenantId = "test-tenant" });

        var row = Assert.Single(await Rows(opened));
        Assert.Equal(source.Id, row.Id);
        var metadata = ReadMetadata(row.Metadata);
        Assert.Equal(source.Id.ToString(), metadata.EventId);
        Assert.Equal(source.Id.ToString(), metadata.Custom[EventSession.SourceEventIdKey]);
        Assert.Equal("backend-contract.translated-source", metadata.Custom[EventSession.SourceEventTypeKey]);
        Assert.Equal("corr-9", metadata.CorrelationId);
        Assert.Equal("cause-9", metadata.CausationId);
        Assert.Equal("test-tenant", metadata.TenantId);
    }

    [ContractFact]
    public async Task NoticeMetadata_OmitsUserAndNetworkFields()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var source = Pass("reduced");
        await opened.Store.AppendAsync(Stream(), [source], metadata: new EventMetadata
        {
            UserId = "user-1",
            UserName = "Ada Lovelace",
            AccountId = "acct-1",
            IpAddress = "203.0.113.8",
            UserAgent = "TestAgent/1",
            AuthorizedBy = "owner",
            AuthorizedAt = DateTime.UtcNow,
            AuthorizationPolicies = ["orders.write"],
            CorrelationId = "corr-reduced",
            CausationId = "cause-reduced",
            TenantId = "test-tenant",
            TraceId = "0af7651916cd43dd8448eb211c80319c",
            SpanId = "b7ad6b7169203331",
            Custom = new Dictionary<string, string> { ["secret"] = "do-not-copy" }
        });

        var row = Assert.Single(await Rows(opened));
        var metadata = ReadMetadata(row.Metadata);
        Assert.Equal("corr-reduced", metadata.CorrelationId);
        Assert.Equal("cause-reduced", metadata.CausationId);
        Assert.Equal("test-tenant", metadata.TenantId);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", metadata.TraceId);
        Assert.Equal("b7ad6b7169203331", metadata.SpanId);
        Assert.Equal("backend-contract.outbox-event", metadata.SchemaName);
        Assert.Equal(1, metadata.SchemaVersion);
        Assert.Null(metadata.UserId);
        Assert.Null(metadata.UserName);
        Assert.Null(metadata.AccountId);
        Assert.Null(metadata.IpAddress);
        Assert.Null(metadata.UserAgent);
        Assert.Null(metadata.AuthorizedBy);
        Assert.Null(metadata.AuthorizedAt);
        Assert.Null(metadata.AuthorizationPolicies);
        Assert.Equal(
            [EventSession.SourceEventIdKey, EventSession.SourceEventTypeKey],
            metadata.Custom.Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.DoesNotContain("Ada Lovelace", row.Metadata, StringComparison.Ordinal);
        Assert.DoesNotContain("203.0.113.8", row.Metadata, StringComparison.Ordinal);
        Assert.DoesNotContain("TestAgent/1", row.Metadata, StringComparison.Ordinal);
        Assert.DoesNotContain("do-not-copy", row.Metadata, StringComparison.Ordinal);
        Assert.DoesNotContain("orders.write", row.Metadata, StringComparison.Ordinal);
    }

    [ContractFact]
    public async Task VersionConflict_LeavesNeitherTheFailedEventNorItsNotice()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var stream = Stream();
        var first = Pass("kept");
        await opened.Store.AppendAsync(stream, [first]);
        var rejected = Pass("rejected");
        await Assert.ThrowsAsync<ConcurrencyException>(() => opened.Store.AppendAsync(
            stream,
            [rejected],
            expectedVersion: 5));

        var stored = Assert.Single(await opened.Store.ReadStreamAsync(stream));
        Assert.Equal(first.Id, stored.Event.Id);
        var rows = await Rows(opened);
        var row = Assert.Single(rows);
        Assert.Equal(first.Id, row.Id);
        Assert.DoesNotContain(rows, candidate => candidate.Id == rejected.Id);
    }

    [ContractFact]
    public async Task OutboxOn_WithNothingDeclared_FailsWhenTheStoreOpens()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var options = ContractStores.Isolated(enableOutbox: true, eventTypes: ContractPublication.SourceTypes);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Backend.OpenEventStoreAsync(options));
        Assert.Contains(options.SchemaName, ex.Message, StringComparison.Ordinal);
        Assert.Contains("backend-contract.outbox-event", ex.Message, StringComparison.Ordinal);
        Assert.Contains("backend-contract.internal-only", ex.Message, StringComparison.Ordinal);
        Assert.Contains("backend-contract.translated-source", ex.Message, StringComparison.Ordinal);
        Assert.Contains("backend-contract.skipped-source", ex.Message, StringComparison.Ordinal);
    }

    [ContractFact]
    public async Task OutboxOff_WritesNoRows()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync(enableOutbox: false);
        var stream = Stream();
        await opened.Store.AppendAsync(stream, [Pass("off"), Translated("off", "still-internal")]);
        Assert.Equal(2, (await opened.Store.ReadStreamAsync(stream)).Count);
        Assert.Empty(await Rows(opened));
    }

    [ContractFact]
    public async Task OnlyTheMiddleEvent_IsWritten()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var stream = Stream();
        var middle = Pass("middle");
        var result = await opened.Store.AppendAsync(stream, [Internal("before"), middle, Internal("after")]);
        Assert.Equal(3, (await opened.Store.ReadStreamAsync(stream)).Count);
        var row = Assert.Single(await Rows(opened));
        Assert.Equal(middle.Id, row.Id);
        Assert.Equal("backend-contract.outbox-event", row.EventType);
        Assert.Equal(result.SequencePositions[1], row.SequencePosition);

        var tag = Tag();
        var dcbMiddle = Pass("dcb-middle");
        var dcb = await opened.Store.AppendAsync(
            [
                new TaggedEvent(Internal("dcb-before"), [tag]),
                new TaggedEvent(dcbMiddle, [tag]),
                new TaggedEvent(Internal("dcb-after"), [tag])
            ],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));
        var rows = await Rows(opened);
        var dcbRow = Assert.Single(rows, candidate => candidate.Id == dcbMiddle.Id);
        Assert.Equal(dcb.SequencePositions[1], dcbRow.SequencePosition);
    }

    [ContractFact]
    public async Task Append_WithNoDeclaredEventInTheBatch_WritesNoRow()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var stream = Stream();
        await opened.Store.AppendAsync(stream, [Internal("alone"), Skipped()]);
        Assert.Equal(2, (await opened.Store.ReadStreamAsync(stream)).Count);
        Assert.Empty(await Rows(opened));
    }

    [ContractFact]
    public async Task RawAppend_WritesARowOnlyWhenTheCallerAttachesANotice()
    {
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await OpenDeclaredAsync();
        var plain = Stream();
        var payload = """{"Kind":"stored"}"""u8.ToArray();
        await opened.Log.AppendAsync(plain, [new AppendEvent("contract.stored.frame", payload, schemaVersion: 1)]);
        Assert.Empty(await Rows(opened));

        var noticed = Stream();
        var noticePayload = """{"Kind":"notice"}"""u8.ToArray();
        var notice = new OutboundEvent(
            "contract.outbox.notice-frame",
            noticePayload,
            """{"EventId":"00000000-0000-0000-0000-000000000001"}"""u8.ToArray(),
            schemaVersion: 2,
            codecId: 2);
        await opened.Log.AppendAsync(noticed, [new AppendEvent(
            "contract.stored.frame",
            payload,
            schemaVersion: 1,
            codecId: EventCodec.Json,
            outbound: notice)]);

        var row = Assert.Single(await Rows(opened));
        Assert.Equal("contract.outbox.notice-frame", row.EventType);
        Assert.Equal(2, row.SchemaVersion);
        Assert.Equal(2, row.CodecId);
        Assert.Equal(noticePayload, row.Payload.ToArray());
        var stored = Assert.Single(await opened.Log.ReadStreamAsync(noticed));
        Assert.Equal("contract.stored.frame", stored.EventType);
        Assert.Equal(1, stored.SchemaVersion);
    }

    private Task<ContractEventStore> OpenDeclaredAsync(bool enableOutbox = true)
        => Backend.OpenEventStoreAsync(ContractStores.Isolated(
            enableOutbox: enableOutbox,
            includeOutboxReader: true,
            eventTypes: ContractPublication.SourceTypes,
            publishedEvents: ContractPublication.Declare));

    private static async Task<IReadOnlyList<OutboxMessage>> Rows(ContractEventStore opened)
    {
        SkipUnless(opened.Outbox is not null, "This backend did not open an outbox.");
        return await opened.Outbox!.GetUnprocessedAsync(20);
    }

    private static string Stream() => "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");

    private static string Tag() => "pub-" + Guid.NewGuid().ToString("N");

    private static ContractOutboxEvent Pass(string value)
        => new() { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = value };

    private static ContractTranslatedSource Translated(string value, string note)
        => new()
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Value = value,
            InternalNote = note
        };

    private static ContractInternalEvent Internal(string secret)
        => new() { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Secret = secret };

    private static ContractSkippedSource Skipped()
        => new() { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow };

    private static EventMetadata ReadMetadata(string json)
    {
        var metadata = JsonSerializer.Deserialize<EventMetadata>(json, MetadataJson);
        Assert.NotNull(metadata);
        return metadata!;
    }
}
