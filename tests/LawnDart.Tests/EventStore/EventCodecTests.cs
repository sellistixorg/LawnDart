using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class EventCodecTests
{
    private static int _nextUserId = EventCodec.UserRangeStart;
    private static readonly object IdGate = new();

    private static byte NextUserId()
    {
        lock (IdGate)
        {
            var id = _nextUserId++;
            if (id > EventCodec.UserRangeEnd)
                throw new InvalidOperationException("User codec id range exhausted in tests.");
            return (byte)id;
        }
    }

    [Theory]
    [InlineData(EventCodec.Json, EventCodec.JsonMime)]
    [InlineData(EventCodec.MemoryPack, EventCodec.MemoryPackMime)]
    [InlineData(EventCodec.Avro, EventCodec.AvroMime)]
    [InlineData(EventCodec.Protobuf, EventCodec.ProtobufMime)]
    public void Built_in_mime_round_trips_id_and_mime(byte id, string mime)
    {
        Assert.Equal(id, EventCodec.IdFor(mime));
        Assert.True(EventCodec.TryGetMime(id, out var resolved));
        Assert.Equal(mime, resolved);
    }

    [Fact]
    public void User_range_register_round_trips()
    {
        var id = NextUserId();
        var mime = $"application/vnd.lawndart.test+{Guid.NewGuid():N}";

        EventCodec.Register(id, mime);

        Assert.Equal(id, EventCodec.IdFor(mime));
        Assert.True(EventCodec.TryGetMime(id, out var resolved));
        Assert.Equal(mime, resolved);

        EventCodec.Register(id, mime);
        Assert.Equal(id, EventCodec.IdFor(mime));
    }

    [Theory]
    [InlineData((byte)0)]
    [InlineData(EventCodec.Json)]
    [InlineData(EventCodec.MemoryPack)]
    [InlineData((byte)63)]
    [InlineData(EventCodec.Opaque)]
    public void Reserved_range_register_throws(byte id)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            EventCodec.Register(id, $"application/vnd.lawndart.reserved+{id}"));
        Assert.Equal("id", ex.ParamName);
        Assert.Contains("Register", ex.Message);
    }

    [Fact]
    public void Conflicting_re_registration_throws()
    {
        var id = NextUserId();
        var mime = $"application/vnd.lawndart.conflict+{Guid.NewGuid():N}";
        EventCodec.Register(id, mime);

        var sameId = Assert.Throws<InvalidOperationException>(() =>
            EventCodec.Register(id, mime + "-other"));
        Assert.Contains(id.ToString(), sameId.Message);

        var otherId = NextUserId();
        var sameMime = Assert.Throws<InvalidOperationException>(() =>
            EventCodec.Register(otherId, mime));
        Assert.Contains(mime, sameMime.Message);
    }

    [Fact]
    public void Unregistered_mime_throws_naming_register()
    {
        var mime = $"application/vnd.lawndart.unknown+{Guid.NewGuid():N}";
        var ex = Assert.Throws<UnregisteredEventCodecException>(() => EventCodec.IdFor(mime));
        Assert.Equal(mime, ex.Mime);
        Assert.Contains(nameof(EventCodec.Register), ex.Message);
    }

    [Fact]
    public void TryGetMime_rejects_zero_and_opaque()
    {
        Assert.False(EventCodec.TryGetMime(0, out _));
        Assert.False(EventCodec.TryGetMime(EventCodec.Opaque, out _));
    }

}
