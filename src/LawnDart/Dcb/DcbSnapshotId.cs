using System.Security.Cryptography;
using System.Text;

namespace LawnDart.Dcb;

/// <summary>
/// Stable snapshot identity for a DCB load-query tag set.
/// </summary>
/// <remarks>
/// All event-store snapshot implementations key DCB snapshots by this value so a tag
/// combination cannot blow a SQL clustered-key limit or produce an illegal Windows
/// filename. The identity is always a SHA-256 hex string — never plaintext joined tags,
/// and never length-conditional.
/// <para>
/// Canonical encoding is length-prefixed UTF-8 of tags sorted with
/// <see cref="StringComparer.Ordinal"/>: a 32-bit little-endian tag count, then each tag
/// as a 32-bit little-endian byte length plus UTF-8 bytes. A single tag <c>"a|b"</c> is
/// therefore distinct from the two-tag set <c>"a"</c>, <c>"b"</c>.
/// </para>
/// </remarks>
public static class DcbSnapshotId
{
    /// <summary>Length of the lowercase SHA-256 hex identity.</summary>
    public const int HexLength = 64;

    /// <summary>
    /// Returns the 64-character lowercase SHA-256 hex of the canonical encoding of
    /// <paramref name="tags"/>.
    /// </summary>
    /// <param name="tags">Load-query tags that define the consistency boundary. Must not be null; elements must not be null.</param>
    /// <returns>A 64-character lowercase hexadecimal string.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="tags"/> or any element is null.</exception>
    public static string FromLoadTags(IReadOnlyList<string> tags)
    {
        ArgumentNullException.ThrowIfNull(tags);

        var sorted = new string[tags.Count];
        for (var i = 0; i < tags.Count; i++)
        {
            ArgumentNullException.ThrowIfNull(tags[i]);
            sorted[i] = tags[i];
        }

        Array.Sort(sorted, StringComparer.Ordinal);

        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(sorted.Length);
            foreach (var tag in sorted)
            {
                var bytes = Encoding.UTF8.GetBytes(tag);
                bw.Write(bytes.Length);
                bw.Write(bytes);
            }
        }

        var hash = SHA256.HashData(ms.ToArray());
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
