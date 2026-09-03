namespace LawnDart.Messaging;

/// <summary>
/// Cross-transport messaging configuration. Registered via <c>AddMessaging()</c>.
/// </summary>
public class MessagingOptions
{
    /// <summary>
    /// Azure Service Bus connection string. Used by the ServiceBus transport package.
    /// </summary>
    public string? ServiceBusConnectionString { get; set; }

    /// <summary>
    /// How long a processed message ID is retained in the inbox store before being eligible for expiry.
    /// Duplicate messages arriving within this window will be discarded.
    /// Defaults to 24 hours.
    /// </summary>
    public TimeSpan InboxDeduplicationWindow { get; set; } = TimeSpan.FromHours(24);
}
