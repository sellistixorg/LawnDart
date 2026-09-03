using System.Text.Json;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Tests.Helpers;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionViewJsonTests
{
    [Fact]
    public void Write_uses_camelCase_property_names()
    {
        var v = new CounterView
        {
            Count        = 1,
            StreamId     = "s",
            LastUpdated  = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)
        };

        var json = JsonSerializer.Serialize(v, typeof(CounterView), ProjectionViewJson.Write);

        Assert.Contains("\"count\":1", json);
        Assert.DoesNotContain("\"Count\":", json);
        Assert.Contains("\"streamId\":\"s\"", json);
    }

    [Fact]
    public void Read_roundTrips_camelCase()
    {
        var v = new CounterView
        {
            Count        = 99,
            StreamId     = "x",
            LastUpdated  = DateTime.UtcNow
        };

        var json = JsonSerializer.Serialize(v, typeof(CounterView), ProjectionViewJson.Write);
        var back = JsonSerializer.Deserialize<CounterView>(json, ProjectionViewJson.Read);

        Assert.NotNull(back);
        Assert.Equal(99, back!.Count);
        Assert.Equal("x", back.StreamId);
    }

    [Fact]
    public void Read_accepts_legacy_PascalCase_snapshots()
    {
        const string legacy =
            """{"Count":3,"StreamId":"abc","LastUpdated":"2020-01-01T00:00:00Z"}""";

        var v = JsonSerializer.Deserialize<CounterView>(legacy, ProjectionViewJson.Read);

        Assert.NotNull(v);
        Assert.Equal(3, v!.Count);
        Assert.Equal("abc", v.StreamId);
    }
}
