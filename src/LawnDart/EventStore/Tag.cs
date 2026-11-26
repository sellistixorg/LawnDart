using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace LawnDart.EventStore;

/// <summary>
/// A typed event tag in <c>key:value</c> form.
/// </summary>
/// <remarks>
/// Build a value with <see cref="Create(string, string)"/> or <see cref="Parse(string)"/>, and read
/// the canonical text with <see cref="ToString"/>. There is no implicit conversion from or to
/// <see cref="string"/>. Rules, the same on every store: key and value are non-empty and have no
/// leading or trailing whitespace; the key may not contain a colon (the first colon splits key and
/// value, so the value may contain colons); the canonical <c>key:value</c> text is at most
/// <see cref="MaxLength"/> characters. Comparison is ordinal and case sensitive.
/// </remarks>
[JsonConverter(typeof(TagJsonConverter))]
public readonly struct Tag : IEquatable<Tag>, IComparable<Tag>
{
    /// <summary>The longest canonical tag text, in characters.</summary>
    public const int MaxLength = 256;

    // The canonical text is kept as is; key and value are sliced from it on demand.
    private readonly string? _text;
    private readonly short _colon;

    private Tag(string text, int colon)
    {
        _text = text;
        _colon = (short)colon;
    }

    /// <summary>The part before the first colon.</summary>
    public string Key => _text is null ? string.Empty : _text[.._colon];

    /// <summary>The part after the first colon. May contain colons.</summary>
    public string Value => _text is null ? string.Empty : _text[(_colon + 1)..];

    /// <summary>Builds a tag from a key and a value.</summary>
    /// <param name="key">Key. Non-empty, no colon, no leading or trailing whitespace.</param>
    /// <param name="value">Value. Non-empty, no leading or trailing whitespace.</param>
    /// <exception cref="ArgumentException">A part breaks the tag rules.</exception>
    public static Tag Create(string key, string value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        var error = Validate(key, value);
        if (error is not null)
            throw new ArgumentException(error, error.StartsWith("Value", StringComparison.Ordinal) ? nameof(value) : nameof(key));
        return new Tag(key + ":" + value, key.Length);
    }

    /// <summary>Parses canonical <c>key:value</c> text.</summary>
    /// <param name="value">Canonical text.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    /// <exception cref="FormatException">The text breaks the tag rules.</exception>
    public static Tag Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var error = TryParseCore(value, out var tag);
        if (error is not null)
        {
            var shown = value.Length <= 64 ? value : value[..64] + "...";
            throw new FormatException($"'{shown}' is not a valid tag. {error}");
        }

        return tag;
    }

    /// <summary>Tries to parse canonical <c>key:value</c> text.</summary>
    /// <param name="value">Canonical text.</param>
    /// <param name="tag">The parsed value, or <see langword="default"/> on failure.</param>
    /// <returns><see langword="true"/> when the text is a valid tag.</returns>
    public static bool TryParse([NotNullWhen(true)] string? value, out Tag tag)
    {
        if (value is null)
        {
            tag = default;
            return false;
        }

        return TryParseCore(value, out tag) is null;
    }

    /// <summary>Returns the canonical <c>key:value</c> text. A <see langword="default"/> value returns an empty string.</summary>
    public override string ToString() => _text ?? string.Empty;

    /// <inheritdoc />
    public bool Equals(Tag other) => string.Equals(_text, other._text, StringComparison.Ordinal);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is Tag other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _text is null ? 0 : StringComparer.Ordinal.GetHashCode(_text);

    /// <inheritdoc />
    public int CompareTo(Tag other) => string.CompareOrdinal(_text, other._text);

    /// <summary>Ordinal equality.</summary>
    public static bool operator ==(Tag left, Tag right) => left.Equals(right);

    /// <summary>Ordinal inequality.</summary>
    public static bool operator !=(Tag left, Tag right) => !left.Equals(right);

    /// <summary>Ordinal less than.</summary>
    public static bool operator <(Tag left, Tag right) => left.CompareTo(right) < 0;

    /// <summary>Ordinal less than or equal.</summary>
    public static bool operator <=(Tag left, Tag right) => left.CompareTo(right) <= 0;

    /// <summary>Ordinal greater than.</summary>
    public static bool operator >(Tag left, Tag right) => left.CompareTo(right) > 0;

    /// <summary>Ordinal greater than or equal.</summary>
    public static bool operator >=(Tag left, Tag right) => left.CompareTo(right) >= 0;

    internal bool IsDefault => _text is null;

    private static string? TryParseCore(string value, out Tag tag)
    {
        tag = default;
        var span = value.AsSpan();
        var colon = span.IndexOf(':');
        if (colon < 0)
            return "Expected key:value.";

        var error = Validate(span[..colon], span[(colon + 1)..]);
        if (error is null)
            tag = new Tag(value, colon);
        return error;
    }

    private static string? Validate(ReadOnlySpan<char> key, ReadOnlySpan<char> value)
    {
        if (key.IsEmpty)
            return "Key is required (tags use key:value).";
        if (char.IsWhiteSpace(key[0]) || char.IsWhiteSpace(key[^1]))
            return "Key may not start or end with whitespace.";
        if (key.Contains(':'))
            return "Key may not contain a colon.";
        if (value.IsEmpty)
            return "Value is required (tags use key:value).";
        if (char.IsWhiteSpace(value[0]) || char.IsWhiteSpace(value[^1]))
            return "Value may not start or end with whitespace.";
        if (key.Length + 1 + value.Length > MaxLength)
            return $"Tag is longer than {MaxLength} characters.";
        return null;
    }
}

/// <summary>Writes and reads a <see cref="Tag"/> as its canonical string.</summary>
internal sealed class TagJsonConverter : JsonConverter<Tag>
{
    public override Tag Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
            throw new JsonException("A tag must be a JSON string.");
        var text = reader.GetString()!;
        return Tag.TryParse(text, out var value)
            ? value
            : throw new JsonException($"'{text}' is not a valid tag.");
    }

    public override void Write(Utf8JsonWriter writer, Tag value, JsonSerializerOptions options)
        => writer.WriteStringValue(value.ToString());

    public override Tag ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var text = reader.GetString()!;
        return Tag.TryParse(text, out var value)
            ? value
            : throw new JsonException($"'{text}' is not a valid tag.");
    }

    public override void WriteAsPropertyName(Utf8JsonWriter writer, Tag value, JsonSerializerOptions options)
        => writer.WritePropertyName(value.ToString());
}
