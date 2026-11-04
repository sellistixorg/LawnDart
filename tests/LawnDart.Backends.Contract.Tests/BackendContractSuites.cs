namespace LawnDart.Backends.Contract.Tests;

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryEventStoreContractTests : EventStoreContractTests
{
    public InMemoryEventStoreContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerEventStoreContractTests : EventStoreContractTests
{
    public SqlServerEventStoreContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresEventStoreContractTests : EventStoreContractTests
{
    public PostgresEventStoreContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryRepositoryTenantContractTests : RepositoryTenantContractTests
{
    public InMemoryRepositoryTenantContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerRepositoryTenantContractTests : RepositoryTenantContractTests
{
    public SqlServerRepositoryTenantContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresRepositoryTenantContractTests : RepositoryTenantContractTests
{
    public PostgresRepositoryTenantContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryDcbContractTests : DcbContractTests
{
    public InMemoryDcbContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerDcbContractTests : DcbContractTests
{
    public SqlServerDcbContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresDcbContractTests : DcbContractTests
{
    public PostgresDcbContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryDcbEntityTagContractTests : DcbEntityTagContractTests
{
    public InMemoryDcbEntityTagContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerDcbEntityTagContractTests : DcbEntityTagContractTests
{
    public SqlServerDcbEntityTagContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresDcbEntityTagContractTests : DcbEntityTagContractTests
{
    public PostgresDcbEntityTagContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryAggregateEventTagContractTests : AggregateEventTagContractTests
{
    public InMemoryAggregateEventTagContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerAggregateEventTagContractTests : AggregateEventTagContractTests
{
    public SqlServerAggregateEventTagContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresAggregateEventTagContractTests : AggregateEventTagContractTests
{
    public PostgresAggregateEventTagContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryStreamRegistryContractTests : StreamRegistryContractTests
{
    public InMemoryStreamRegistryContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerStreamRegistryContractTests : StreamRegistryContractTests
{
    public SqlServerStreamRegistryContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresStreamRegistryContractTests : StreamRegistryContractTests
{
    public PostgresStreamRegistryContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryStreamListContractTests : StreamListContractTests
{
    public InMemoryStreamListContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerStreamListContractTests : StreamListContractTests
{
    public SqlServerStreamListContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresStreamListContractTests : StreamListContractTests
{
    public PostgresStreamListContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemorySubscriptionContractTests : SubscriptionContractTests
{
    public InMemorySubscriptionContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerSubscriptionContractTests : SubscriptionContractTests
{
    public SqlServerSubscriptionContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresSubscriptionContractTests : SubscriptionContractTests
{
    public PostgresSubscriptionContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresPollOnlySubscriptionContractTests : SubscriptionContractTests
{
    public PostgresPollOnlySubscriptionContractTests(PostgresContractFixture fixture) : base(fixture.PollOnlyBackend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryReadHorizonContractTests : ReadHorizonContractTests
{
    public InMemoryReadHorizonContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerReadHorizonContractTests : ReadHorizonContractTests
{
    public SqlServerReadHorizonContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresReadHorizonContractTests : ReadHorizonContractTests
{
    public PostgresReadHorizonContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryTagContractTests : TagContractTests
{
    public InMemoryTagContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerTagContractTests : TagContractTests
{
    public SqlServerTagContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresTagContractTests : TagContractTests
{
    public PostgresTagContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemorySnapshotContractTests : SnapshotContractTests
{
    public InMemorySnapshotContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerSnapshotContractTests : SnapshotContractTests
{
    public SqlServerSnapshotContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresSnapshotContractTests : SnapshotContractTests
{
    public PostgresSnapshotContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryOutboxContractTests : OutboxContractTests
{
    public InMemoryOutboxContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerOutboxContractTests : OutboxContractTests
{
    public SqlServerOutboxContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresOutboxContractTests : OutboxContractTests
{
    public PostgresOutboxContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryInboxContractTests : InboxContractTests
{
    public InMemoryInboxContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerInboxContractTests : InboxContractTests
{
    public SqlServerInboxContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresInboxContractTests : InboxContractTests
{
    public PostgresInboxContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryViewStoreContractTests : ViewStoreContractTests
{
    public InMemoryViewStoreContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerViewStoreContractTests : ViewStoreContractTests
{
    public SqlServerViewStoreContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresViewStoreContractTests : ViewStoreContractTests
{
    public PostgresViewStoreContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryCheckpointStoreContractTests : CheckpointStoreContractTests
{
    public InMemoryCheckpointStoreContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerCheckpointStoreContractTests : CheckpointStoreContractTests
{
    public SqlServerCheckpointStoreContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresCheckpointStoreContractTests : CheckpointStoreContractTests
{
    public PostgresCheckpointStoreContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryProjectionHostContractTests : ProjectionHostContractTests
{
    public InMemoryProjectionHostContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerProjectionHostContractTests : ProjectionHostContractTests
{
    public SqlServerProjectionHostContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresProjectionHostContractTests : ProjectionHostContractTests
{
    public PostgresProjectionHostContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryPerEventTagContractTests : PerEventTagContractTests
{
    public InMemoryPerEventTagContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerPerEventTagContractTests : PerEventTagContractTests
{
    public SqlServerPerEventTagContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresPerEventTagContractTests : PerEventTagContractTests
{
    public PostgresPerEventTagContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryRepeatedTagContractTests : RepeatedTagContractTests
{
    public InMemoryRepeatedTagContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerRepeatedTagContractTests : RepeatedTagContractTests
{
    public SqlServerRepeatedTagContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresRepeatedTagContractTests : RepeatedTagContractTests
{
    public PostgresRepeatedTagContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryDcbAppendRaceContractTests : DcbAppendRaceContractTests
{
    public InMemoryDcbAppendRaceContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerDcbAppendRaceContractTests : DcbAppendRaceContractTests
{
    public SqlServerDcbAppendRaceContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresDcbAppendRaceContractTests : DcbAppendRaceContractTests
{
    public PostgresDcbAppendRaceContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerEventNameTableContractTests : EventNameTableContractTests
{
    public SqlServerEventNameTableContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresEventNameTableContractTests : EventNameTableContractTests
{
    public PostgresEventNameTableContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerStreamTagTableContractTests : StreamTagTableContractTests
{
    public SqlServerStreamTagTableContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresStreamTagTableContractTests : StreamTagTableContractTests
{
    public PostgresStreamTagTableContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}
