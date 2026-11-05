using System.Security.Cryptography;
using System.Text;
using LawnDart.EventStore;

namespace LawnDart.Dcb;

/// <summary>
/// Stable snapshot identity for a dynamic consistency boundary decision query.
/// </summary>
/// <remarks>
/// All event-store snapshot implementations key these snapshots by this value.
/// The identity is always a 64-character lowercase SHA-256 hex string.
/// <para>
/// Canonical bytes, version 1: a version byte <c>1</c>, then the item count as a
/// 32-bit little-endian integer, then each item's own canonical bytes. Items are
/// sorted by those bytes, so item order does not change the id. A repeated type
/// or tag inside one item is kept once. Two identical items hash as one item.
/// For each item the bytes are the type count, then each type sorted with
/// <see cref="StringComparer.Ordinal"/> as a 32-bit little-endian byte length plus
/// UTF-8, then the tag count and each tag the same way.
/// </para>
/// </remarks>
public static class DcbSnapshotId
{
    /// <summary>Length of the lowercase SHA-256 hex identity.</summary>
    public const int HexLength = 64;

    /// <summary>
    /// Returns the 64-character lowercase SHA-256 hex of the canonical encoding of
    /// <paramref name="query"/>.
    /// </summary>
    /// <param name="query">Decision query. Must not be null. Items, types, and tags must not be null.</param>
    /// <returns>A 64-character lowercase hexadecimal string.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="query"/>, an item, or a type or tag is null.</exception>
    public static string FromQuery(Query query)
    {
        ArgumentNullException.ThrowIfNull(query);

        var itemBytes = new List<byte[]>(query.Items.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in query.Items)
        {
            var bytes = CanonicalItem(item);
            if (seen.Add(Convert.ToHexString(bytes)))
                itemBytes.Add(bytes);
        }

        itemBytes.Sort(CompareBytes);

        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write((byte)1);
            bw.Write(itemBytes.Count);
            foreach (var item in itemBytes)
                bw.Write(item);
        }

        return Convert.ToHexString(SHA256.HashData(ms.ToArray())).ToLowerInvariant();
    }

    private static byte[] CanonicalItem(QueryItem item)
    {
        ArgumentNullException.ThrowIfNull(item);
        var types = DistinctSorted(item.Types);
        var tags = DistinctSorted(item.Tags);

        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(types.Length);
            foreach (var type in types)
                WriteUtf8(bw, type);
            bw.Write(tags.Length);
            foreach (var tag in tags)
                WriteUtf8(bw, tag);
        }

        return ms.ToArray();
    }

    private static string[] DistinctSorted(IReadOnlyList<string>? values)
    {
        if (values is null || values.Count == 0)
            return [];

        var distinct = new List<string>(values.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var value in values)
        {
            ArgumentNullException.ThrowIfNull(value);
            if (seen.Add(value))
                distinct.Add(value);
        }

        distinct.Sort(StringComparer.Ordinal);
        return distinct.ToArray();
    }

    private static void WriteUtf8(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    private static int CompareBytes(byte[] left, byte[] right)
    {
        var length = Math.Min(left.Length, right.Length);
        for (var i = 0; i < length; i++)
        {
            var cmp = left[i].CompareTo(right[i]);
            if (cmp != 0)
                return cmp;
        }

        return left.Length.CompareTo(right.Length);
    }
}
