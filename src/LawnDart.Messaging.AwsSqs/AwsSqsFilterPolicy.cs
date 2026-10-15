using System.Text;
using System.Text.Json;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// SNS subscription filter on <see cref="AwsSqsNames.EventTypeAttribute"/>.
/// The policy lists the handled event types and matches a message that omits the attribute.
/// A later update adds types. It does not remove one unless replace is requested.
/// The missing-attribute match stays, so a filter change does not start dropping those messages.
/// </summary>
public static class AwsSqsFilterPolicy
{
    /// <summary>Maximum length of an SNS filter policy, in characters.</summary>
    public const int MaxLength = 256 * 1024;

    /// <summary>
    /// Builds the filter for <paramref name="messageTypes"/>.
    /// The policy is <c>{"lawndart_type":[{"exists":false},"type-a",...]}</c>.
    /// String values are sorted. <c>exists: false</c> is always included.
    /// </summary>
    /// <param name="messageTypes">Declared event type names this subscription handles.</param>
    public static string Build(IReadOnlyCollection<string> messageTypes)
    {
        ArgumentNullException.ThrowIfNull(messageTypes);
        var names = Normalize(messageTypes);
        if (names.Count == 0)
        {
            throw new ArgumentException(
                "At least one message type is required.",
                nameof(messageTypes));
        }

        var json = Write(names);
        if (json.Length > MaxLength)
        {
            throw new InvalidOperationException(
                $"SNS filter policy is {json.Length} characters. The limit is {MaxLength}. " +
                "Use fewer event types on one topic, or shorter names.");
        }

        return json;
    }

    /// <summary>
    /// Unions <paramref name="requested"/> into <paramref name="current"/>.
    /// When <paramref name="replace"/> is false, types already on the policy stay.
    /// <c>exists: false</c> stays in either case. That match is what lets a message
    /// without <c>lawndart_type</c> through, and removing it would drop those messages.
    /// </summary>
    /// <param name="current">Policy JSON already on the subscription. <c>null</c> when none is set.</param>
    /// <param name="requested">Event types this process handles.</param>
    /// <param name="replace">When true, the policy lists <paramref name="requested"/> only.</param>
    public static string Merge(string? current, IReadOnlyCollection<string> requested, bool replace)
    {
        ArgumentNullException.ThrowIfNull(requested);
        var types = new HashSet<string>(StringComparer.Ordinal);
        if (!replace && !string.IsNullOrWhiteSpace(current))
        {
            foreach (var name in ParseTypes(current))
                types.Add(name);
        }

        foreach (var name in Normalize(requested))
            types.Add(name);

        return Build(types);
    }

    /// <summary>
    /// <c>true</c> when <paramref name="policy"/> includes <c>exists: false</c> and every name in
    /// <paramref name="requested"/>. When <paramref name="replace"/> is true, the policy must list
    /// those names and no others.
    /// </summary>
    /// <param name="policy">Policy JSON. <c>null</c> does not cover.</param>
    /// <param name="requested">Event types that must be present.</param>
    /// <param name="replace">When true, extra types fail the check.</param>
    public static bool Covers(string? policy, IReadOnlyCollection<string> requested, bool replace)
    {
        ArgumentNullException.ThrowIfNull(requested);
        if (!AllowsMissingEventType(policy))
            return false;

        var have = new HashSet<string>(ParseTypes(policy!), StringComparer.Ordinal);
        var need = new HashSet<string>(Normalize(requested), StringComparer.Ordinal);
        if (need.Count == 0)
            return false;

        return replace ? have.SetEquals(need) : need.IsSubsetOf(have);
    }

    /// <summary>
    /// <c>true</c> when the policy matches a message that omits <c>lawndart_type</c>.
    /// SNS still does not deliver a message that has no attributes at all.
    /// <c>exists: false</c> matches only when some other attribute is present.
    /// </summary>
    /// <param name="policy">Policy JSON. May be null.</param>
    public static bool AllowsMissingEventType(string? policy)
    {
        if (string.IsNullOrWhiteSpace(policy))
            return false;

        using var document = JsonDocument.Parse(policy);
        if (!document.RootElement.TryGetProperty(AwsSqsNames.EventTypeAttribute, out var array) ||
            array.ValueKind != JsonValueKind.Array)
        {
            return false;
        }

        foreach (var item in array.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object)
                continue;
            if (item.TryGetProperty("exists", out var exists) && exists.ValueKind == JsonValueKind.False)
                return true;
        }

        return false;
    }

    /// <summary>Declared event type names listed as strings on <c>lawndart_type</c>.</summary>
    /// <param name="policy">Policy JSON.</param>
    public static IReadOnlyList<string> ParseTypes(string policy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policy);
        try
        {
            using var document = JsonDocument.Parse(policy);
            if (!document.RootElement.TryGetProperty(AwsSqsNames.EventTypeAttribute, out var array) ||
                array.ValueKind != JsonValueKind.Array)
            {
                return [];
            }

            var names = new List<string>();
            foreach (var item in array.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.String)
                    continue;
                var text = item.GetString();
                if (!string.IsNullOrWhiteSpace(text))
                    names.Add(text);
            }

            return names;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                "SNS filter policy is not JSON LawnDart can merge. Leave the policy unset or let this transport write it.",
                ex);
        }
    }

    private static List<string> Normalize(IReadOnlyCollection<string> messageTypes)
        => messageTypes
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToList();

    private static string Write(IReadOnlyList<string> names)
    {
        var builder = new StringBuilder();
        builder.Append("{\"");
        builder.Append(AwsSqsNames.EventTypeAttribute);
        builder.Append("\":[{\"exists\":false}");
        foreach (var name in names)
        {
            builder.Append(',');
            builder.Append(JsonSerializer.Serialize(name));
        }

        builder.Append("]}");
        return builder.ToString();
    }
}
