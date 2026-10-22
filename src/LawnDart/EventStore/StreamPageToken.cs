using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace LawnDart.EventStore;

/// <summary>
/// Opaque page token shared by the shipped stores.
/// Bytes: version, an 8-byte filter hash, the UTF-8 length, the last stream id.
/// </summary>
internal static class StreamPageToken
{
    private const byte Version = 1;
    private const int HashLength = 8;
    private const int MaxStreamIdBytes = 2048;

    private static readonly Encoding Utf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    internal static string? ReadAfter(string? pageToken, StreamListFilter? filter)
    {
        if (string.IsNullOrEmpty(pageToken))
            return null;

        byte[] bytes;
        try
        {
            bytes = FromBase64Url(pageToken);
        }
        catch (FormatException ex)
        {
            throw new ArgumentException("Page token is not valid.", nameof(pageToken), ex);
        }

        if (bytes.Length < 1 + HashLength + 4 + 1 || bytes[0] != Version)
            throw new ArgumentException("Page token is not valid.", nameof(pageToken));

        var expected = Hash(filter);
        if (!bytes.AsSpan(1, HashLength).SequenceEqual(expected))
            throw new ArgumentException("Page token does not match this filter.", nameof(pageToken));

        var length = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(1 + HashLength, 4));
        if (length < 1 || length > MaxStreamIdBytes || 1 + HashLength + 4 + length != bytes.Length)
            throw new ArgumentException("Page token is not valid.", nameof(pageToken));

        try
        {
            var streamId = Utf8.GetString(bytes, 1 + HashLength + 4, length);
            if (streamId.Length == 0)
                throw new ArgumentException("Page token is not valid.", nameof(pageToken));
            return streamId;
        }
        catch (ArgumentException)
        {
            throw new ArgumentException("Page token is not valid.", nameof(pageToken));
        }
    }

    internal static string Encode(string streamId, StreamListFilter? filter)
    {
        var idBytes = Utf8.GetBytes(streamId);
        var bytes = new byte[1 + HashLength + 4 + idBytes.Length];
        bytes[0] = Version;
        Hash(filter).CopyTo(bytes.AsSpan(1));
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1 + HashLength, 4), idBytes.Length);
        idBytes.CopyTo(bytes.AsSpan(1 + HashLength + 4));
        return ToBase64Url(bytes);
    }

    internal static void CheckPageSize(int pageSize)
    {
        if (pageSize < 1 || pageSize > StreamPage.MaxPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"Page size must be from 1 to {StreamPage.MaxPageSize}.");
        }
    }

    internal static StreamPage ToPage(IReadOnlyList<StreamMetadata> rows, int pageSize, StreamListFilter? filter)
    {
        if (rows.Count <= pageSize)
            return new StreamPage { Streams = rows, NextPageToken = null };

        var page = new StreamMetadata[pageSize];
        for (var i = 0; i < pageSize; i++)
            page[i] = rows[i];

        return new StreamPage
        {
            Streams = page,
            NextPageToken = Encode(page[pageSize - 1].StreamId, filter)
        };
    }

    internal static string? Normalize(string? value)
        => string.IsNullOrEmpty(value) ? null : value;

    private static byte[] Hash(StreamListFilter? filter)
    {
        using var buffer = new MemoryStream();
        buffer.WriteByte(1);
        Write(buffer, Normalize(filter?.Prefix));
        Write(buffer, Normalize(filter?.Tag));
        Write(buffer, Normalize(filter?.AggregateType));
        var digest = SHA256.HashData(buffer.GetBuffer().AsSpan(0, (int)buffer.Length));
        return digest.AsSpan(0, HashLength).ToArray();
    }

    private static void Write(Stream buffer, string? value)
    {
        if (value is null)
        {
            buffer.WriteByte(0);
            return;
        }

        buffer.WriteByte(1);
        var bytes = Utf8.GetBytes(value);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
        buffer.Write(length);
        buffer.Write(bytes);
    }

    private static string ToBase64Url(byte[] bytes)
    {
        var encoded = Convert.ToBase64String(bytes);
        return encoded.TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    private static byte[] FromBase64Url(string token)
    {
        var padded = token.Replace('-', '+').Replace('_', '/');
        var remainder = padded.Length % 4;
        if (remainder == 1)
            throw new FormatException("Page token is not valid base64url.");
        if (remainder > 0)
            padded = padded.PadRight(padded.Length + (4 - remainder), '=');
        return Convert.FromBase64String(padded);
    }
}
