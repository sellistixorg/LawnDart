using LawnDart.EventSourcing.Postgres;

namespace LawnDart.Backends.Contract.Tests;

public class PostgresFunctionMarkerTests
{
    [Fact]
    public void MarkerVersion_MatchesOnlyTheExactNumber()
    {
        const string prefix = "LawnDart:append_to_stream:";
        Assert.True(PostgresSchemaProbe.MarkerVersionIs("-- " + prefix + "1", prefix, 1));
        Assert.False(PostgresSchemaProbe.MarkerVersionIs("-- " + prefix + "2", prefix, 1));
        Assert.False(PostgresSchemaProbe.MarkerVersionIs("-- " + prefix + "10", prefix, 1));
    }
}
