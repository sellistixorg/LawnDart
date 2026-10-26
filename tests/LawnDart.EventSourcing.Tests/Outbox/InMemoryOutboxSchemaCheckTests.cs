using LawnDart.EventSourcing.Outbox;

namespace LawnDart.EventSourcing.Tests.Outbox;

public class InMemoryOutboxSchemaCheckTests
{
    [Fact]
    public async Task VerifySchemaAsync_ReturnsAMatchWithoutADatabase()
    {
        var writer = new InMemoryOutboxWriter();

        var result = await writer.VerifySchemaAsync();

        Assert.True(result.Matches);
        Assert.Null(result.ExpectedFormat);
        Assert.Null(result.FoundFormat);
        Assert.Empty(result.Missing);
        result.ThrowIfMismatch();
    }
}
