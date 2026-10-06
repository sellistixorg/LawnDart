using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;
using LawnDart.Sql;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

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
    public void ViewStore_QualifiesConfiguredSchema()
    {
        var store = new SqlViewStore("Server=localhost;Database=test;", schemaName: "odd]schema");
        Assert.Equal("[odd]]schema].[ProjectionViews]", store.QualifiedTableName);
    }

    [Fact]
    public void CheckpointStore_QualifiesConfiguredSchema()
    {
        var store = new SqlCheckpointStore(
            "Server=localhost;Database=test;",
            new InMemoryViewStore(),
            schemaName: "odd]schema");
        Assert.Equal("[odd]]schema].[ProjectionCheckpoints]", store.QualifiedTableName);
    }
}
