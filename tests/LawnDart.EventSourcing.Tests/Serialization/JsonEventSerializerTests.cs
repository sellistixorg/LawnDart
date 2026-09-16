using System.Text;
using LawnDart.EventSourcing.Serialization;

namespace LawnDart.EventSourcing.Tests.Serialization;

public class JsonEventSerializerTests
{
    [Fact]
    public void Serialize_returns_utf8_json_bytes_not_a_string_or_base64()
    {
        var serializer = new JsonEventSerializer();
        var evt = new Payload("Ada");

        var bytes = serializer.Serialize(evt, evt.GetType());
        var json = Encoding.UTF8.GetString(bytes.Span);

        Assert.Equal("application/json", serializer.ContentType);
        Assert.Contains("\"Name\":\"Ada\"", json);
        Assert.DoesNotContain("base64", json, StringComparison.OrdinalIgnoreCase);

        var roundTrip = Assert.IsType<Payload>(serializer.Deserialize(bytes, typeof(Payload)));
        Assert.Equal("Ada", roundTrip.Name);
    }

    private sealed record Payload(string Name);
}
