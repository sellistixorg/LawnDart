using System.Text.Json;
using System.Text.Json.Serialization;

namespace LawnDart.Messaging.Tests;

public class MessagingJsonTests
{
    [Fact]
    public void Options_are_one_read_only_PascalCase_setting()
    {
        Assert.Same(MessagingJson.Options, MessagingJson.Options);
        Assert.True(MessagingJson.Options.IsReadOnly);
        Assert.Null(MessagingJson.Options.PropertyNamingPolicy);
        Assert.True(MessagingJson.Options.PropertyNameCaseInsensitive);
        Assert.Equal(JsonIgnoreCondition.WhenWritingNull, MessagingJson.Options.DefaultIgnoreCondition);

        var json = JsonSerializer.Serialize(new Sample(7, "ada", null), MessagingJson.Options);
        Assert.Contains("\"Number\":7", json, StringComparison.Ordinal);
        Assert.Contains("\"Name\":\"ada\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("Note", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"number\"", json, StringComparison.Ordinal);
    }

    [Fact]
    public void Options_reject_mutation()
    {
        Assert.Throws<InvalidOperationException>(() =>
            MessagingJson.Options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
    }

    private sealed record Sample(int Number, string Name, string? Note);
}
