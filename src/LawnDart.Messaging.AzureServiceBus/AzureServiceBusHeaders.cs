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
    /// SQL filter for one subscription.
    /// A publish matches the first comparison. A retry matches the second only on the subscription that scheduled it.
    /// </summary>
    /// <param name="subscriptionName">Consumer name.</param>
    public static string RetryFilter(string subscriptionName)
    {
        var escaped = subscriptionName.Replace("'", "''", StringComparison.Ordinal);
        return $"user.{TargetSubscription} = '{TargetAll}' OR user.{TargetSubscription} = '{escaped}'";
    }

    public static Dictionary<string, string> ToEnvelope(IReadOnlyDictionary<string, object> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in properties)
        {
            if (pair.Key is Attempt or TargetSubscription || pair.Value is null)
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
