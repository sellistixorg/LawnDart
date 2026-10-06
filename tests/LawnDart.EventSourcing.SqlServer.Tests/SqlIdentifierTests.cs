using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.Sql;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests;

public class SqlIdentifierTests
{
    [Fact]
    public void Quote_DoublesClosingBracket()
        => Assert.Equal("[a]]b]", SqlIdentifier.Quote("a]b"));

    [Fact]
    public void Quote_WrapsPlainName()
        => Assert.Equal("[plain]", SqlIdentifier.Quote("plain"));

    [Fact]
    public void Qualify_EscapesEachPart()
        => Assert.Equal("[we]]ird].[t]]1]", SqlIdentifier.Qualify("we]ird", "t]1"));

    [Fact]
    public void Literal_DoublesQuote()
        => Assert.Equal("o''brien", SqlIdentifier.Literal("o'brien"));

    [Fact]
    public void Literal_EscapesQuotedIdentifier()
        => Assert.Equal("[x]]''y]", SqlIdentifier.Literal(SqlIdentifier.Quote("x]'y")));

    [Fact]
    public void EventStore_QualifiesConfiguredSchema()
    {
        var options = new SqlServerEventStoreOptions { SchemaName = "odd]schema" };
        var store = new SqlServerEventStore("Server=localhost;Database=test;", options: options);

        Assert.Equal("[odd]]schema].[Events]", store.QualifiedEventsTable);
        Assert.Equal("[odd]]schema].[EventTags]", store.QualifiedTagsTable);
        Assert.Equal("[odd]]schema].[EventTypes]", store.QualifiedTypesTable);
        Assert.Equal("[odd]]schema].[Streams]", store.QualifiedRegistryTable);
        Assert.Equal("[odd]]schema].[EventSequencePosition]", store.QualifiedSequence);
    }

    [Fact]
    public void OutboxWriter_QualifiesConfiguredSchema()
    {
        var writer = new SqlServerOutboxWriter("Server=localhost;Database=test;", schemaName: "odd]schema");
        Assert.Equal("[odd]]schema].[Outbox]", writer.QualifiedTableName);
    }
}

[Trait("Category", "Integration")]
public class OddSchemaNameTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlContainer = new MsSqlBuilder(MsSqlTestImage.Server2022)
        .WithPassword("Test123!")
        .Build();

    private string? _connectionString;

    public async Task InitializeAsync()
    {
        await _sqlContainer.StartAsync();
        _connectionString = _sqlContainer.GetConnectionString();
    }

    public async Task DisposeAsync()
        => await _sqlContainer.DisposeAsync();

    [Fact]
    public async Task InitializeSchemaAsync_SchemaNameWithClosingBracket_CreatesObjects()
    {
        var options = new SqlServerEventStoreOptions
        {
            SchemaName = "odd]schema",
            RequireTenantId = false
        };
        var store = new SqlServerEventStore(_connectionString!, options: options);
        await store.InitializeSchemaAsync();
    }
}
