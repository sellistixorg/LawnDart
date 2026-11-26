using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace LawnDart.EventStore;

/// <summary>
/// A typed event stream identifier.
/// </summary>
/// <remarks>
/// <para>
/// The canonical text is <c>{type}:{id}</c>, <c>{tenant}:{type}:{id}</c>, or the reserved
/// dynamic consistency boundary form <c>dcb</c> / <c>{tenant}:dcb</c>. Build a value with
/// <see cref="For(string, string)"/>, <see cref="For(string, string, string)"/>,
/// <see cref="Dcb(string?)"/>, or <see cref="Parse(string)"/>, and read the canonical text
/// with <see cref="ToString"/>. There is no implicit conversion from or to <see cref="string"/>.
/// </para>
/// <para>
/// Rules: type and id are non-empty and have no leading or trailing whitespace; a tenant, when
/// present, follows the same rule. Tenant and type may not contain colons. In the three-part
/// form the id is the remainder after the type, so it may contain colons. In the two-part form
/// the id may not contain a colon (the text would read back as a three-part id), and the id may
/// not be <c>dcb</c> (that text is the tenant's reserved stream). The canonical text is at most
/// <see cref="MaxLength"/> characters. Comparison is ordinal and case sensitive.
/// </para>
/// </remarks>
[JsonConverter(typeof(StreamIdJsonConverter))]
public readonly struct StreamId : IEquatable<StreamId>, IComparable<StreamId>
{
    /// <summary>The longest canonical stream id text, in characters.</summary>
    public const int MaxLength = 255;

    internal const string DcbType = "dcb";

    // The canonical text is kept as is; parts are sliced from it on demand, so parsing
    // stored text allocates nothing beyond the value itself.
    private readonly string? _text;
    private readonly short _tenantLength; // -1 when there is no tenant
    private readonly short _typeLength;

    private StreamId(string text, int tenantLength, int typeLength, bool isDcb)
    {
        _text = text;
        _tenantLength = (short)tenantLength;
        _typeLength = (short)typeLength;
        IsDcb = isDcb;
    }

    /// <summary>The tenant part, or <see langword="null"/> when the stream has no tenant.</summary>
    public string? TenantId => _text is null || _tenantLength < 0 ? null : _text[.._tenantLength];

    /// <summary>The stream type (the aggregate type for an aggregate stream). <c>dcb</c> for the reserved form.</summary>
    public string Type => _text is null ? string.Empty : _text.Substring(TypeStart, _typeLength);

    /// <summary>The id part. Empty for the reserved dynamic consistency boundary form.</summary>
    public string Id
    {
        get
        {
            if (_text is null || IsDcb)
                return string.Empty;
            return _text[(TypeStart + _typeLength + 1)..];
        }
    }

    /// <summary><see langword="true"/> for the reserved dynamic consistency boundary stream <c>dcb</c> or <c>{tenant}:dcb</c>.</summary>
    public bool IsDcb { get; }

    private int TypeStart => _tenantLength < 0 ? 0 : _tenantLength + 1;

    /// <summary>Builds a stream id with no tenant: <c>{type}:{id}</c>.</summary>
    /// <param name="type">Stream type. No colons, no leading or trailing whitespace.</param>
    /// <param name="id">Stream id. No colons, no leading or trailing whitespace, not <c>dcb</c>.</param>
    /// <exception cref="ArgumentException">A part breaks the stream id rules.</exception>
    public static StreamId For(string type, string id)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(id);
        var error = ValidateParts(ReadOnlySpan<char>.Empty, hasTenant: false, type, id);
        if (error is not null)
            throw new ArgumentException(error.Value.Message, error.Value.Part == Part.Type ? nameof(type) : nameof(id));
        return new StreamId(type + ":" + id, -1, type.Length, isDcb: false);
    }

    /// <summary>Builds a stream id with a tenant: <c>{tenant}:{type}:{id}</c>.</summary>
    /// <param name="tenantId">Tenant. No colons, no leading or trailing whitespace.</param>
    /// <param name="type">Stream type. No colons, no leading or trailing whitespace.</param>
    /// <param name="id">Stream id. No leading or trailing whitespace. May contain colons.</param>
    /// <exception cref="ArgumentException">A part breaks the stream id rules.</exception>
    public static StreamId For(string tenantId, string type, string id)
    {
        ArgumentNullException.ThrowIfNull(tenantId);
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(id);
        var error = ValidateParts(tenantId, hasTenant: true, type, id);
        if (error is not null)
        {
            var param = error.Value.Part switch
            {
                Part.Tenant => nameof(tenantId),
                Part.Type => nameof(type),
                _ => nameof(id)
            };
            throw new ArgumentException(error.Value.Message, param);
        }

        return new StreamId(tenantId + ":" + type + ":" + id, tenantId.Length, type.Length, isDcb: false);
    }

    /// <summary>
    /// Builds the reserved dynamic consistency boundary stream id: <c>dcb</c> with no tenant,
    /// or <c>{tenant}:dcb</c> with a tenant.
    /// </summary>
    /// <param name="tenantId">Tenant, or <see langword="null"/> or empty for no tenant.</param>
    /// <exception cref="ArgumentException">The tenant breaks the stream id rules.</exception>
    public static StreamId Dcb(string? tenantId = null)
    {
        if (string.IsNullOrEmpty(tenantId))
            return new StreamId(DcbType, -1, DcbType.Length, isDcb: true);

        var error = ValidatePart(tenantId, "Tenant", allowColon: false);
        if (error is not null)
            throw new ArgumentException(error, nameof(tenantId));
        if (tenantId.Length + 1 + DcbType.Length > MaxLength)
            throw new ArgumentException($"Stream id is longer than {MaxLength} characters.", nameof(tenantId));

        return new StreamId(tenantId + ":" + DcbType, tenantId.Length, DcbType.Length, isDcb: true);
    }

    /// <summary>Parses canonical stream id text.</summary>
    /// <param name="value">Canonical text.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    /// <exception cref="FormatException">The text breaks the stream id rules.</exception>
    public static StreamId Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var error = TryParseCore(value, out var streamId);
        if (error is not null)
            throw new FormatException($"'{Truncate(value)}' is not a valid stream id. {error}");
        return streamId;
    }

    /// <summary>Tries to parse canonical stream id text.</summary>
    /// <param name="value">Canonical text.</param>
    /// <param name="streamId">The parsed value, or <see langword="default"/> on failure.</param>
    /// <returns><see langword="true"/> when the text is a valid stream id.</returns>
    public static bool TryParse([NotNullWhen(true)] string? value, out StreamId streamId)
    {
        if (value is null)
        {
            streamId = default;
            return false;
        }

        return TryParseCore(value, out streamId) is null;
    }

    /// <summary>Returns the canonical text. A <see langword="default"/> value returns an empty string.</summary>
    public override string ToString() => _text ?? string.Empty;

    /// <inheritdoc />
    public bool Equals(StreamId other) => string.Equals(_text, other._text, StringComparison.Ordinal);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is StreamId other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _text is null ? 0 : StringComparer.Ordinal.GetHashCode(_text);

    /// <inheritdoc />
    public int CompareTo(StreamId other) => string.CompareOrdinal(_text, other._text);

    /// <summary>Ordinal equality.</summary>
    public static bool operator ==(StreamId left, StreamId right) => left.Equals(right);

    /// <summary>Ordinal inequality.</summary>
    public static bool operator !=(StreamId left, StreamId right) => !left.Equals(right);

    /// <summary>Ordinal less than.</summary>
    public static bool operator <(StreamId left, StreamId right) => left.CompareTo(right) < 0;

    /// <summary>Ordinal less than or equal.</summary>
    public static bool operator <=(StreamId left, StreamId right) => left.CompareTo(right) <= 0;

    /// <summary>Ordinal greater than.</summary>
    public static bool operator >(StreamId left, StreamId right) => left.CompareTo(right) > 0;

    /// <summary>Ordinal greater than or equal.</summary>
    public static bool operator >=(StreamId left, StreamId right) => left.CompareTo(right) >= 0;

    internal bool IsDefault => _text is null;

    private enum Part { Tenant, Type, Id }

    private static string? TryParseCore(string value, out StreamId streamId)
    {
        streamId = default;
        if (value.Length == 0)
            return "Stream id is empty.";
        if (value.Length > MaxLength)
            return $"Stream id is longer than {MaxLength} characters.";

        if (string.Equals(value, DcbType, StringComparison.Ordinal))
        {
            streamId = new StreamId(value, -1, DcbType.Length, isDcb: true);
            return null;
        }

        var span = value.AsSpan();
        var first = span.IndexOf(':');
        if (first < 0)
            return "Expected {type}:{id} or {tenant}:{type}:{id}.";

        var head = span[..first];
        var rest = span[(first + 1)..];

        if (rest.SequenceEqual(DcbType))
        {
            var tenantError = ValidatePart(head, "Tenant", allowColon: false);
            if (tenantError is not null)
                return tenantError;
            streamId = new StreamId(value, first, DcbType.Length, isDcb: true);
            return null;
        }

        var second = rest.IndexOf(':');
        if (second < 0)
        {
            var twoPartError = ValidateParts(ReadOnlySpan<char>.Empty, hasTenant: false, head, rest);
            if (twoPartError is not null)
                return twoPartError.Value.Message;
            streamId = new StreamId(value, -1, first, isDcb: false);
            return null;
        }

        var error = ValidateParts(head, hasTenant: true, rest[..second], rest[(second + 1)..]);
        if (error is not null)
            return error.Value.Message;
        streamId = new StreamId(value, first, second, isDcb: false);
        return null;
    }

    private static (Part Part, string Message)? ValidateParts(
        ReadOnlySpan<char> tenantId, bool hasTenant, ReadOnlySpan<char> type, ReadOnlySpan<char> id)
    {
        if (hasTenant && ValidatePart(tenantId, "Tenant", allowColon: false) is { } tenantError)
            return (Part.Tenant, tenantError);

        if (ValidatePart(type, "Type", allowColon: false) is { } typeError)
            return (Part.Type, typeError);

        if (!hasTenant && id.Contains(':'))
            return (Part.Id, "Id may not contain a colon in a stream id with no tenant (it would read back as {tenant}:{type}:{id}).");

        if (ValidatePart(id, "Id", allowColon: true) is { } idError)
            return (Part.Id, idError);

        if (!hasTenant && id.SequenceEqual(DcbType))
            return (Part.Id, "Id 'dcb' is reserved in a stream id with no tenant ({tenant}:dcb is the tenant's dynamic consistency boundary stream). Use StreamId.Dcb.");

        var length = (hasTenant ? tenantId.Length + 1 : 0) + type.Length + 1 + id.Length;
        if (length > MaxLength)
            return (Part.Id, $"Stream id is longer than {MaxLength} characters.");

        return null;
    }

    private static string? ValidatePart(ReadOnlySpan<char> part, string name, bool allowColon)
    {
        if (part.IsEmpty)
            return $"{name} is required.";
        if (char.IsWhiteSpace(part[0]) || char.IsWhiteSpace(part[^1]))
            return $"{name} may not start or end with whitespace.";
        if (!allowColon && part.Contains(':'))
            return $"{name} may not contain a colon.";
        return null;
    }

    private static string Truncate(string value) => value.Length <= 64 ? value : value[..64] + "...";
}

/// <summary>Writes and reads a <see cref="StreamId"/> as its canonical string.</summary>
internal sealed class StreamIdJsonConverter : JsonConverter<StreamId>
{
    public override StreamId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
            throw new JsonException("A stream id must be a JSON string.");
        var text = reader.GetString()!;
        return StreamId.TryParse(text, out var value)
            ? value
            : throw new JsonException($"'{text}' is not a valid stream id.");
    }

    public override void Write(Utf8JsonWriter writer, StreamId value, JsonSerializerOptions options)
        => writer.WriteStringValue(value.ToString());

    public override StreamId ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var text = reader.GetString()!;
        return StreamId.TryParse(text, out var value)
            ? value
            : throw new JsonException($"'{text}' is not a valid stream id.");
    }

    public override void WriteAsPropertyName(Utf8JsonWriter writer, StreamId value, JsonSerializerOptions options)
        => writer.WritePropertyName(value.ToString());
}
