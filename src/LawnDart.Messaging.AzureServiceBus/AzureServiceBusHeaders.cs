using System.Globalization;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Reads Service Bus application properties into envelope strings.
/// The attempt counter stays off <see cref="MessageContext.Headers"/>.
/// </summary>
internal static class AzureServiceBusHeaders
{
    /// <summary>Completed handler attempts carried on a rescheduled delivery. The value is <c>lawndart-attempt</c>.</summary>
    public const string Attempt = "lawndart-attempt";

    /// <summary>
    /// Application property that selects which subscription receives a message.
    /// A publish sets <see cref="TargetAll"/>. A retry sets the subscription name.
    /// The value is <c>lawndart_target</c>.
    /// </summary>
    public const string TargetSubscription = "lawndart_target";

    /// <summary>Value of <see cref="TargetSubscription"/> on a publish. Every subscription accepts it.</summary>
    public const string TargetAll = "*";

    /// <summary>Rule that keeps a retry on the subscription that failed.</summary>
    public const string RouteRuleName = "lawndart-route";

    /// <summary>
    /// Application property that carries the declared event type.
    /// A publish sets it. A subscription rule matches it so a shared topic
    /// delivers each type only to the subscriptions that handle it.
    /// The value is <c>lawndart_type</c>.
    /// </summary>
    public const string EventType = AzureServiceBusNames.EventTypeProperty;

    /// <summary>Maximum length of a Service Bus SQL filter expression.</summary>
    public const int SqlFilterMaxLength = 1024;

    /// <summary>
    /// SQL filter for the <see cref="TargetSubscription"/> property.
    /// A missing property or <see cref="TargetAll"/> matches every subscription.
    /// A retry matches only the subscription named in the property.
    /// </summary>
    /// <param name="subscriptionName">Consumer name.</param>
    public static string RetryFilter(string subscriptionName)
    {
        var escaped = Escape(subscriptionName);
        return $"user.{TargetSubscription} IS NULL OR user.{TargetSubscription} = '{TargetAll}' OR user.{TargetSubscription} = '{escaped}'";
    }

    /// <summary>
    /// SQL filter for one subscription.
    /// The target clause is <see cref="RetryFilter"/>.
    /// The type clause matches a missing <see cref="EventType"/> or one of <paramref name="messageTypes"/>.
    /// </summary>
    /// <param name="subscriptionName">Consumer name.</param>
    /// <param name="messageTypes">Declared event type names this subscription handles.</param>
    public static string RouteFilter(string subscriptionName, IReadOnlyCollection<string> messageTypes)
    {
        ArgumentNullException.ThrowIfNull(messageTypes);
        var types = messageTypes
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .Select(static name => $"user.{EventType} = '{Escape(name)}'")
            .ToArray();
        if (types.Length == 0)
        {
            throw new ArgumentException(
                "At least one message type is required.",
                nameof(messageTypes));
        }

        var expression = $"({RetryFilter(subscriptionName)}) AND (user.{EventType} IS NULL OR {string.Join(" OR ", types)})";
        if (expression.Length > SqlFilterMaxLength)
        {
            throw new InvalidOperationException(
                $"Azure Service Bus SQL filter is {expression.Length} characters. The limit is {SqlFilterMaxLength}. " +
                "Use fewer event types on one topic, or shorter names.");
        }

        return expression;
    }

    private static string Escape(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);

    /// <summary>
    /// Declared event type names named by <c>user.lawndart_type = '...'</c> in <paramref name="sql"/>.
    /// A target-only rule has none.
    /// </summary>
    /// <param name="sql">SQL filter text. May be null.</param>
    internal static IReadOnlyList<string> EventTypesInSql(string? sql)
    {
        if (string.IsNullOrEmpty(sql))
            return [];

        const string marker = "user.lawndart_type = '";
        var names = new List<string>();
        var index = 0;
        while ((index = sql.IndexOf(marker, index, StringComparison.Ordinal)) >= 0)
        {
            var start = index + marker.Length;
            var end = start;
            while (end < sql.Length)
            {
                if (sql[end] == '\'')
                {
                    if (end + 1 < sql.Length && sql[end + 1] == '\'')
                    {
                        end += 2;
                        continue;
                    }

                    break;
                }

                end++;
            }

            if (end >= sql.Length)
                break;

            var raw = sql[start..end].Replace("''", "'", StringComparison.Ordinal);
            if (raw.Length > 0)
                names.Add(raw);
            index = end + 1;
        }

        return names;
    }

    public static Dictionary<string, string> ToEnvelope(IReadOnlyDictionary<string, object> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in properties)
        {
            if (pair.Key is Attempt or TargetSubscription or EventType || pair.Value is null)
                continue;

            var text = ToText(pair.Value);
            if (text is not null)
                result[pair.Key] = text;
        }

        return result;
    }

    /// <summary>How many handler attempts have already finished.</summary>
    public static int CompletedAttempts(IReadOnlyDictionary<string, object> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (!properties.TryGetValue(Attempt, out var value) || value is null)
            return 0;

        try
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            throw new InvalidOperationException(
                $"Application property '{Attempt}' could not be read. The runtime type was '{value.GetType().FullName}'.",
                ex);
        }
    }

    private static string? ToText(object value) => value switch
    {
        string text => text,
        byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => System.Text.Encoding.UTF8.GetString(memory.Span),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };
}
