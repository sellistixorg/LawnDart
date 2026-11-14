using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.EventStore;

namespace LawnDart.SqlPerformance;

internal sealed class DatasetPlan
{
    public const long OneMillion = 1_000_000;
    public const long TenMillion = 10_000_000;
    public const long OneHundredMillion = 100_000_000;
    public const long OneBillion = 1_000_000_000;
    public const int SnapshotEvery = 50;
    public const int DefaultSeed = 1;
    public const int DefaultTenants = 8;

    public static DatasetPlan Default { get; } = new(OneMillion, DefaultSeed, DefaultTenants);

    public DatasetPlan(long size, int seed, int tenants)
    {
        if (size < 1)
            throw new ArgumentOutOfRangeException(nameof(size), "Size must be at least one event.");
        if (tenants < 1)
            throw new ArgumentOutOfRangeException(nameof(tenants), "At least one tenant is required.");
        Size = size;
        Seed = seed;
        Tenants = tenants;
    }

    public long Size { get; }

    public int Seed { get; }

    public int Tenants { get; }

    public ulong MixSeed => unchecked((ulong)(uint)Seed) ^ 0xC0FFEEUL;
}

internal static class SizeParser
{
    public static long Parse(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            throw new ArgumentException("Size is required.");

        var value = text.Trim();
        if (value.Equals("1M", StringComparison.OrdinalIgnoreCase))
            return DatasetPlan.OneMillion;
        if (value.Equals("10M", StringComparison.OrdinalIgnoreCase))
            return DatasetPlan.TenMillion;
        if (value.Equals("100M", StringComparison.OrdinalIgnoreCase))
            return DatasetPlan.OneHundredMillion;
        if (value.Equals("1B", StringComparison.OrdinalIgnoreCase))
            return DatasetPlan.OneBillion;
        if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var count) && count > 0)
            return count;

        throw new ArgumentException("Size must be an event count, 1M, 10M, 100M, or 1B.");
    }
}

internal static class SplitMix
{
    public static ulong Mix(ulong seed, ulong a, ulong b)
    {
        var z = seed + (0x9E3779B97F4A7C15UL * (a + 1)) + (b * 0xBF58476D1CE4E5B9UL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}

internal static class TagPicker
{
    public const int Vocabulary = 2000;
    private static readonly double[] Cumulative = Build();

    public static string Pick(ulong seed, long ordinal, ulong salt)
    {
        var drawn = SplitMix.Mix(seed, 13, unchecked((ulong)ordinal + (salt * 0x9E3779B97F4A7C15UL)));
        var u = (drawn % 1_000_000_000UL) / 1_000_000_000d;
        var lo = 0;
        var hi = Cumulative.Length - 1;
        while (lo < hi)
        {
            var mid = (lo + hi) / 2;
            if (u > Cumulative[mid])
                lo = mid + 1;
            else
                hi = mid;
        }

        return Rank(lo);
    }

    public static string Rank(int rank) => "sku:" + rank.ToString(CultureInfo.InvariantCulture);

    public static string[] DcbTags(ulong seed, long ordinal, string eventType)
    {
        var count = 1 + (int)(SplitMix.Mix(seed, 11, unchecked((ulong)ordinal)) % 3UL);
        var tags = new List<string>(count);
        if (eventType is "note-added" or "price-changed")
            tags.Add(Rank(0));
        if (eventType == "price-changed" && count >= 2)
            tags.Add(Rank(1));

        var salt = 1UL;
        while (tags.Count < count && salt < 40)
        {
            var tag = Pick(seed, ordinal, salt);
            salt++;
            if (!tags.Contains(tag, StringComparer.Ordinal))
                tags.Add(tag);
        }

        while (tags.Count < count)
            tags.Add(Rank(Vocabulary + tags.Count));

        return tags.ToArray();
    }

    private static double[] Build()
    {
        var weights = new double[Vocabulary];
        double sum = 0;
        for (var i = 0; i < Vocabulary; i++)
        {
            sum += 1.0 / (i + 1);
            weights[i] = sum;
        }

        for (var i = 0; i < Vocabulary; i++)
            weights[i] /= sum;
        return weights;
    }
}

internal static class PayloadFactory
{
    private static readonly int[] Sizes =
    [
        300, 300, 320, 360, 400, 420, 480, 512,
        640, 768, 800, 1024, 1536, 2048, 3072, 4096
    ];

    public static byte[][] Build(string[] eventTypes)
    {
        var payloads = new byte[Sizes.Length * eventTypes.Length][];
        var n = 0;
        foreach (var eventType in eventTypes)
        {
            for (var variant = 0; variant < Sizes.Length; variant++)
                payloads[n++] = Make(eventType, variant, Sizes[variant]);
        }

        return payloads;
    }

    public static byte[] Make(string eventType, int variant, int size)
    {
        var prefix = "{\"t\":\"" + eventType + "\",\"v\":\""
            + variant.ToString("00", CultureInfo.InvariantCulture) + "\",\"p\":\"";
        const string suffix = "\"}";
        var pad = size - prefix.Length - suffix.Length;
        if (pad < 1)
            throw new InvalidOperationException("Payload size is below the JSON frame.");

        var text = prefix + new string('x', pad) + suffix;
        var bytes = Encoding.UTF8.GetBytes(text);
        if (bytes.Length != size)
            throw new InvalidOperationException("Payload length does not match the requested size.");
        return bytes;
    }
}

internal sealed class EventRow
{
    public required string Tenant { get; init; }
    public required string StreamId { get; init; }
    public long Version { get; init; }
    public long Position { get; init; }
    public Guid EventId { get; init; }
    public required string EventType { get; init; }
    public int SchemaVersion { get; init; } = 1;
    public byte CodecId { get; init; } = EventCodec.Json;
    public required byte[] Payload { get; init; }
    public required string MetadataJson { get; init; }
    public required string TagsJson { get; init; }
    public required string[] Tags { get; init; }
    public int PartitionHash { get; init; }
    public DateTime Timestamp { get; init; }
    public bool IsDcb { get; init; }
    public string AggregateType { get; init; } = "";
    public Guid AggregateId { get; init; }
    public bool ClosesStream { get; init; }
    public long StreamEventCount { get; init; }
    public long StreamVersion { get; init; }
    public DateTime StreamOpenedAt { get; init; }
    public string StreamTagsJson { get; init; } = "[]";
    public string[] StreamTags { get; init; } = [];
    public bool WritesSnapshot { get; init; }
    public long SnapshotVersion { get; init; }
}

internal sealed class LoadBatch
{
    public List<EventRow> Events { get; } = [];
}

internal sealed class StreamSample
{
    public string Id { get; set; } = "";
    public long Version { get; set; }
    public long SnapshotVersion { get; set; }
    public string[] Tags { get; set; } = [];
}

internal sealed class DatasetCatalog
{
    public long Size { get; set; }
    public int Seed { get; set; }
    public int Tenants { get; set; }
    public long MaxPosition { get; set; }
    public string HotStreamId { get; set; } = "";
    public long HotVersion { get; set; }
    public string[] HotTags { get; set; } = [];
    public string QueryTag { get; set; } = "sku:0";
    public string QueryType { get; set; } = "note-added";
    public string MultiTagA { get; set; } = "sku:0";
    public string MultiTagB { get; set; } = "sku:1";
    public List<StreamSample> Streams { get; set; } = [];

    public void Save(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, JsonSerializer.Serialize(this, CatalogJson.Options));
    }

    public static DatasetCatalog Load(string path)
        => JsonSerializer.Deserialize<DatasetCatalog>(File.ReadAllText(path), CatalogJson.Options)
           ?? throw new InvalidOperationException("The saved catalog is empty.");
}

internal static class CatalogJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };
}

internal static class SnapshotText
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static (string Json, byte[] Bytes, string Checksum) For(long version)
    {
        var json = JsonSerializer.Serialize(new BenchState { Version = version, Applied = version }, Options);
        var bytes = Encoding.UTF8.GetBytes(json);
        var checksum = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        return (json, bytes, checksum);
    }
}

public sealed class BenchState
{
    public long Version { get; set; }
    public long Applied { get; set; }
}

internal sealed class EventGenerator
{
    public const string QueryTag = "sku:0";
    public const string QueryType = "note-added";
    public const string MultiTagA = "sku:0";
    public const string MultiTagB = "sku:1";

    private static readonly string[] AggregateTypes = ["order", "payment", "shipment"];
    private static readonly DateTime Epoch = new(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    private static readonly Dictionary<string, string[]> EventsByType = new(StringComparer.Ordinal)
    {
        ["order"] = ["order-placed", "order-shipped"],
        ["payment"] = ["payment-taken", "payment-refunded"],
        ["shipment"] = ["shipment-packed", "shipment-sent"]
    };

    private static readonly string[] DcbEventTypes = ["note-added", "stock-moved", "price-changed"];

    private readonly DatasetPlan _plan;
    private readonly int[] _lengths;
    private readonly long[] _ends;
    private readonly byte[][] _payloads;
    private readonly string[] _metadata;

    public EventGenerator(DatasetPlan plan)
    {
        _plan = plan;
        _lengths = BuildLengths(plan, out _ends);
        var names = EventsByType.Values.SelectMany(x => x).Concat(DcbEventTypes).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        _payloads = PayloadFactory.Build(names);
        _metadata = new string[plan.Tenants];
        for (var i = 0; i < plan.Tenants; i++)
            _metadata[i] = "{\"TenantId\":\"" + Tenant(i) + "\"}";
    }

    public static string[] EventTypeNames { get; } =
    [
        "note-added",
        "order-placed",
        "order-shipped",
        "payment-refunded",
        "payment-taken",
        "price-changed",
        "shipment-packed",
        "shipment-sent",
        "stock-moved"
    ];

    public DatasetCatalog Catalog(int sampleLimit = 4096)
    {
        var catalog = new DatasetCatalog
        {
            Size = _plan.Size,
            Seed = _plan.Seed,
            Tenants = _plan.Tenants,
            MaxPosition = _plan.Size,
            QueryTag = QueryTag,
            QueryType = QueryType,
            MultiTagA = MultiTagA,
            MultiTagB = MultiTagB
        };

        var hot = 0;
        for (var i = 1; i < _lengths.Length; i++)
        {
            if (_lengths[i] > _lengths[hot])
                hot = i;
        }

        if (_lengths.Length > 0)
        {
            catalog.HotStreamId = StreamId(hot);
            catalog.HotVersion = _lengths[hot];
            catalog.HotTags = StreamTags(hot);
        }

        var preferred = new List<int>();
        var rest = new List<int>();
        for (var i = 0; i < _lengths.Length; i++)
        {
            var snapshot = SnapshotVersion(_lengths[i]);
            if (snapshot >= 1 && snapshot < _lengths[i])
                preferred.Add(i);
            else
                rest.Add(i);
        }

        foreach (var list in new[] { preferred, rest })
        {
            foreach (var i in list)
            {
                if (catalog.Streams.Count >= sampleLimit)
                    break;
                var snapshot = SnapshotVersion(_lengths[i]);
                catalog.Streams.Add(new StreamSample
                {
                    Id = StreamId(i),
                    Version = _lengths[i],
                    SnapshotVersion = snapshot,
                    Tags = StreamTags(i)
                });
            }
        }

        return catalog;
    }

    public IEnumerable<LoadBatch> Batches(int batchSize)
    {
        if (batchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(batchSize));

        var batch = new LoadBatch();
        for (long index = 0; index < _plan.Size; index++)
        {
            batch.Events.Add(Create(index));
            if (batch.Events.Count >= batchSize)
            {
                yield return batch;
                batch = new LoadBatch();
            }
        }

        if (batch.Events.Count > 0)
            yield return batch;
    }

    public EventRow Create(long index)
    {
        var position = index + 1;
        var stamp = Epoch.AddTicks(position * TimeSpan.TicksPerSecond);
        var eventId = GuidFor(3, unchecked((ulong)position));
        var payload = _payloads[(int)(SplitMix.Mix(_plan.MixSeed, 19, unchecked((ulong)index)) % (ulong)_payloads.Length)];

        if (index % 5 == 4)
            return CreateDcb(index / 5, position, stamp, eventId, payload);

        var ordinal = index - (index / 5);
        var stream = FindStream(ordinal);
        var start = stream == 0 ? 0 : _ends[stream - 1];
        var version = ordinal - start + 1;
        var length = _lengths[stream];
        var tenantIndex = (int)(stream % _plan.Tenants);
        var tenant = Tenant(tenantIndex);
        var aggregateType = AggregateTypes[stream % AggregateTypes.Length];
        var aggregateId = GuidFor(1, unchecked((ulong)stream));
        var streamId = tenant + ":" + aggregateType + ":" + aggregateId.ToString("D");
        var names = EventsByType[aggregateType];
        var eventType = names[(int)((version - 1) % names.Length)];
        var tags = StreamTags(stream);
        var snapshot = SnapshotVersion(length);
        return new EventRow
        {
            Tenant = tenant,
            StreamId = streamId,
            Version = version,
            Position = position,
            EventId = eventId,
            EventType = eventType,
            Payload = payload,
            MetadataJson = _metadata[tenantIndex],
            TagsJson = TagsJson(tags),
            Tags = tags,
            PartitionHash = PartitionHashUtility.GetDeterministicHashCode(streamId),
            Timestamp = stamp,
            IsDcb = false,
            AggregateType = aggregateType,
            AggregateId = aggregateId,
            ClosesStream = version == length,
            StreamEventCount = length,
            StreamVersion = length,
            StreamOpenedAt = Epoch.AddTicks(PositionOfOrdinal(start) * TimeSpan.TicksPerSecond),
            StreamTagsJson = TagsJson(tags),
            StreamTags = tags,
            WritesSnapshot = snapshot > 0 && version == snapshot,
            SnapshotVersion = snapshot
        };
    }

    private EventRow CreateDcb(long ordinal, long position, DateTime stamp, Guid eventId, byte[] payload)
    {
        var tenantIndex = (int)(ordinal % _plan.Tenants);
        var tenant = Tenant(tenantIndex);
        var aggregateId = GuidFor(2, unchecked((ulong)ordinal));
        var streamId = tenant + ":dcb:" + aggregateId.ToString("D");
        var eventType = DcbEventTypes[(int)(ordinal % DcbEventTypes.Length)];
        var tags = TagPicker.DcbTags(_plan.MixSeed, ordinal, eventType);
        return new EventRow
        {
            Tenant = tenant,
            StreamId = streamId,
            Version = 0,
            Position = position,
            EventId = eventId,
            EventType = eventType,
            Payload = payload,
            MetadataJson = _metadata[tenantIndex],
            TagsJson = TagsJson(tags),
            Tags = tags,
            PartitionHash = PartitionHashUtility.GetDeterministicHashCode(streamId),
            Timestamp = stamp,
            IsDcb = true,
            AggregateType = "dcb",
            AggregateId = aggregateId,
            ClosesStream = true,
            StreamEventCount = 1,
            StreamVersion = 0,
            StreamOpenedAt = stamp,
            StreamTagsJson = TagsJson(tags),
            StreamTags = tags,
            WritesSnapshot = false,
            SnapshotVersion = 0
        };
    }

    private string StreamId(int stream)
    {
        var tenant = Tenant((int)(stream % _plan.Tenants));
        var aggregateType = AggregateTypes[stream % AggregateTypes.Length];
        var aggregateId = GuidFor(1, unchecked((ulong)stream));
        return tenant + ":" + aggregateType + ":" + aggregateId.ToString("D");
    }

    private string[] StreamTags(int stream)
    {
        var tenant = Tenant((int)(stream % _plan.Tenants));
        var aggregateType = AggregateTypes[stream % AggregateTypes.Length];
        var aggregateId = GuidFor(1, unchecked((ulong)stream));
        return [ "tenant:" + tenant, aggregateType + ":" + aggregateId.ToString("N") ];
    }

    private string Tenant(int index) => "t" + (index % _plan.Tenants).ToString(CultureInfo.InvariantCulture);

    private Guid GuidFor(ulong kind, ulong index)
    {
        Span<byte> bytes = stackalloc byte[16];
        var a = SplitMix.Mix(_plan.MixSeed, kind, index);
        var b = SplitMix.Mix(_plan.MixSeed, kind ^ 0xA5A5A5A5A5A5A5A5UL, index ^ 0x1234UL);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes, a);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes[8..], b);
        bytes[7] = (byte)((bytes[7] & 0x0F) | 0x40);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes);
    }

    private int FindStream(long ordinal)
    {
        var lo = 0;
        var hi = _ends.Length - 1;
        while (lo < hi)
        {
            var mid = (lo + hi) / 2;
            if (ordinal >= _ends[mid])
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    private static int[] BuildLengths(DatasetPlan plan, out long[] ends)
    {
        var aggregate = plan.Size - (plan.Size / 5);
        var lengths = new List<int>();
        var left = aggregate;
        var stream = 0UL;
        while (left > 0)
        {
            var length = 5 + (int)(SplitMix.Mix(plan.MixSeed, 4, stream) % 196UL);
            if (length > left)
                length = (int)left;
            lengths.Add(length);
            left -= length;
            stream++;
        }

        if (lengths.Count == 0)
            lengths.Add(0);

        ends = new long[lengths.Count];
        long cursor = 0;
        for (var i = 0; i < lengths.Count; i++)
        {
            cursor += lengths[i];
            ends[i] = cursor;
        }

        return lengths.ToArray();
    }

    private static int SnapshotVersion(int length)
    {
        if (length < DatasetPlan.SnapshotEvery)
            return 0;
        return (length / DatasetPlan.SnapshotEvery) * DatasetPlan.SnapshotEvery;
    }

    private static long PositionOfOrdinal(long ordinal) => (ordinal + (ordinal / 4)) + 1;

    private static string TagsJson(string[] tags) => JsonSerializer.Serialize(tags);
}

internal static class GeneratorChecks
{
    public static void AssertStable()
    {
        var plan = new DatasetPlan(20_000, seed: 7, tenants: 4);
        var first = Fingerprint(plan);
        var second = Fingerprint(plan);
        if (first != second)
            throw new InvalidOperationException("The generator changed between two passes with the same seed.");

        var other = Fingerprint(new DatasetPlan(20_000, seed: 8, tenants: 4));
        if (other == first)
            throw new InvalidOperationException("Two seeds produced the same rows.");

        AssertShape(plan);
        var catalog = new EventGenerator(plan).Catalog();
        if (catalog.Streams.Count < 1)
            throw new InvalidOperationException("The catalog has no stream with a tail after its snapshot.");
        if (catalog.HotVersion < 1 || string.IsNullOrEmpty(catalog.HotStreamId))
            throw new InvalidOperationException("The catalog has no hot stream.");
        if (catalog.MaxPosition != plan.Size)
            throw new InvalidOperationException("The catalog max position does not match the size.");
    }

    public static void AssertShape(DatasetPlan plan)
    {
        var generator = new EventGenerator(plan);
        long count = 0;
        long dcb = 0;
        var versions = new Dictionary<string, long>(StringComparer.Ordinal);
        var closed = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var batch in generator.Batches(1024))
        {
            foreach (var row in batch.Events)
            {
                count++;
                if (row.Position != count)
                    throw new InvalidOperationException("Positions are not contiguous.");
                if (row.Payload.Length < 300 || row.Payload.Length > 4096)
                    throw new InvalidOperationException("A payload is outside the requested range.");
                if (row.Tags.Length != row.Tags.Distinct(StringComparer.Ordinal).Count())
                    throw new InvalidOperationException("An event repeats a tag.");
                if (row.EventId == Guid.Empty)
                    throw new InvalidOperationException("An event id is empty.");

                if (row.IsDcb)
                {
                    dcb++;
                    if (row.Version != 0)
                        throw new InvalidOperationException("A dynamic consistency boundary event has a non zero version.");
                    if (row.Tags.Length < 1 || row.Tags.Length > 3)
                        throw new InvalidOperationException("A dynamic consistency boundary event has the wrong number of tags.");
                    if (!row.StreamId.Contains(":dcb:", StringComparison.Ordinal))
                        throw new InvalidOperationException("A dynamic consistency boundary stream id has the wrong shape.");
                    if (row.EventType == "note-added" && row.Tags[0] != "sku:0")
                        throw new InvalidOperationException("A note event is missing its tag.");
                    if (row.EventType == "price-changed" && row.Tags.Length >= 2
                        && (row.Tags[0] != "sku:0" || row.Tags[1] != "sku:1"))
                        throw new InvalidOperationException("A price event is missing its tag pair.");
                }
                else
                {
                    if (row.Version < 1)
                        throw new InvalidOperationException("An aggregate version starts below one.");
                    if (!versions.TryGetValue(row.StreamId, out var previous))
                    {
                        if (row.Version != 1)
                            throw new InvalidOperationException("An aggregate stream does not start at version one.");
                    }
                    else if (row.Version != previous + 1)
                    {
                        throw new InvalidOperationException("Aggregate versions are not contiguous.");
                    }

                    versions[row.StreamId] = row.Version;
                    if (row.Tags.Length != 2)
                        throw new InvalidOperationException("An aggregate event does not carry its own two tags.");
                }

                if (row.ClosesStream)
                    closed[row.StreamId] = row.Version;
            }
        }

        if (count != plan.Size)
            throw new InvalidOperationException("The generator did not produce the requested number of events.");
        if (dcb != plan.Size / 5)
            throw new InvalidOperationException("The dynamic consistency boundary share is wrong.");
        foreach (var pair in versions)
        {
            if (!closed.TryGetValue(pair.Key, out var end) || end != pair.Value)
                throw new InvalidOperationException("A stream did not close on its last version.");
        }
    }

    public static ulong Fingerprint(DatasetPlan plan)
    {
        ulong fp = 0;
        foreach (var batch in new EventGenerator(plan).Batches(1024))
        {
            foreach (var row in batch.Events)
            {
                fp = SplitMix.Mix(fp, unchecked((ulong)row.Position), unchecked((ulong)row.Version));
                fp ^= (ulong)row.Payload.Length << 17;
                fp ^= (ulong)row.Tags.Length << 3;
                fp ^= SplitMix.Mix(fp, Hash(row.StreamId), Hash(row.EventType));
            }
        }

        return fp;
    }

    private static ulong Hash(string value)
    {
        ulong h = 0;
        foreach (var ch in value)
            h = SplitMix.Mix(h, ch, 1);
        return h;
    }
}
