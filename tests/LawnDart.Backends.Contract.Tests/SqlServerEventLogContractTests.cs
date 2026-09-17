using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;

namespace LawnDart.Backends.Contract.Tests;

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerEventLogContractTests : IAsyncLifetime
{
    private readonly SqlServerContractFixture _sql;
    private SqlServerEventStore _store = null!;

    public SqlServerEventLogContractTests(SqlServerContractFixture sql) => _sql = sql;

    public async Task InitializeAsync()
    {
        _store = new SqlServerEventStore(
            _sql.ConnectionString,
            serializer: null,
            tableName: "Events",
            registryTableName: "Streams",
            enableRegistry: true,
            options: new SqlServerEventStoreOptions
            {
                RequireTenantId = false,
                SchemaName = "eventlog"
            });
        await _store.InitializeSchemaAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private IEventLog Log => _store;

    [Fact]
    public Task Thesis_HostWithoutClrTypes_CanReadFilterAndCopy()
        => EventLogContract.Thesis_HostWithoutClrTypes_CanReadFilterAndCopyAsync(Log);

    [Fact]
    public Task AppendRead_SamePayloadWithoutHydrate()
        => EventLogContract.AppendRead_SamePayloadWithoutHydrateAsync(Log);

    [Fact]
    public Task Query_TokenAndTags_WithoutHydrate()
        => EventLogContract.Query_TokenAndTags_WithoutHydrateAsync(Log);

    [Fact]
    public Task TypedAdapter_RegisteredType_StillRoundTrips()
        => EventLogContract.TypedAdapter_RegisteredType_StillRoundTripsAsync(Log);

    [Fact]
    public Task TypedAdapter_UnknownFamily_FailsClosed()
        => EventLogContract.TypedAdapter_UnknownFamily_FailsClosedAsync(Log);

    [Fact]
    public Task TypedAdapter_ContentTypeMismatch_FailsClosed()
        => EventLogContract.TypedAdapter_ContentTypeMismatch_FailsClosedAsync(Log);

    [Fact]
    public Task Append_CodecId2_ReadsCodecId2()
        => EventLogContract.Append_CodecId2_ReadsCodecId2Async(Log);
}
