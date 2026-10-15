namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// What to do when a publish reaches a topic that has no SQS subscription.
/// SNS accepts the publish either way. The message is not queued.
/// </summary>
public enum AwsSqsUnroutablePublish
{
    /// <summary>
    /// Log a warning and return. The publish is not recorded as a success.
    /// </summary>
    Warn = 0,

    /// <summary>
    /// Throw. The caller, including the outbox, still has the message.
    /// </summary>
    Throw = 1,
}

/// <summary>
/// Connection settings for <see cref="AwsSqsMessageTransport"/>.
/// Includes the shared subscription, prefetch, retry, and dead-letter settings.
/// </summary>
public sealed class AwsSqsTransportOptions : MessageTransportOptions
{
    /// <summary>SQS allows a per-message delay of at most 15 minutes.</summary>
    public const int MaxDelaySeconds = 900;

    /// <summary>SQS long polling waits at most 20 seconds.</summary>
    public const int MaxLongPollSeconds = 20;

    /// <summary>SQS visibility timeout is at most 12 hours.</summary>
    public const int MaxVisibilitySeconds = 43_200;

    /// <summary>SQS returns at most 10 messages per receive.</summary>
    public const int MaxMessagesPerReceive = 10;

    /// <summary>SQS <c>maxReceiveCount</c> on a redrive policy is at most 1000.</summary>
    public const int MaxReceiveCountLimit = 1000;

    /// <summary>An SQS or SNS message, including attributes, is at most 256 KiB.</summary>
    public const int MaxMessageBytes = 256 * 1024;

    /// <summary>
    /// AWS region, such as <c>us-east-1</c>.
    /// Used when <see cref="ServiceUrl"/> is not set. The AWS SDK default chain supplies the region when this is empty.
    /// When <see cref="ServiceUrl"/> is set and this is empty, the client uses <c>us-east-1</c> as the signing region.
    /// </summary>
    public string? Region { get; set; }

    /// <summary>
    /// Endpoint override, such as <c>http://localhost:4566</c> for LocalStack.
    /// When this is set, the client talks to that URL instead of the regional AWS endpoint.
    /// Credentials still come from the AWS SDK default chain unless an <c>IAmazonSQS</c> client is registered.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// How long a failed or cancelled delivery waits before it is sent to the same queue again.
    /// SQS delay is whole seconds, from 1 to 15 minutes. A delay under one second is sent as 1 second.
    /// Defaults to 5 seconds. The value must be greater than zero and at most 15 minutes.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long an AWS call and an internal wait may run before they time out.
    /// Defaults to 30 seconds. The value must be greater than zero.
    /// A receive waits the shorter of this and <see cref="LongPollWait"/>.
    /// </summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long a receive call waits for a message. SQS allows 0 to 20 seconds.
    /// Defaults to 20 seconds. The value must be zero or greater and at most 20 seconds.
    /// </summary>
    public TimeSpan LongPollWait { get; set; } = TimeSpan.FromSeconds(MaxLongPollSeconds);

    /// <summary>
    /// How long a received message stays hidden while a handler runs.
    /// Defaults to 30 seconds. The value must be greater than zero and at most 12 hours.
    /// While the handler runs, the transport extends this timeout at about half the interval.
    /// Extensions stop when the handler returns, when one fails and is logged, or after
    /// <see cref="MaxVisibilityRenewals"/> extensions.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How many times visibility may be extended for one delivery.</summary>
    public const int MaxVisibilityRenewals = 240;

    /// <summary>
    /// How many times SQS may deliver one message before it moves that message to the
    /// subscription dead-letter queue. Used only when <see cref="MessageTransportOptions.DeadLetterFailedMessages"/>
    /// is true. Defaults to 10. Valid values are 1 to <see cref="MaxReceiveCountLimit"/>.
    /// A delayed retry is a new message, so this count is a backstop for a receive that
    /// was never deleted, such as a process crash. It is separate from
    /// <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
    /// </summary>
    public int MaxReceiveCount { get; set; } = 10;

    /// <summary>
    /// How many times a message for a recorded type waits for its handler before that wait counts as a failed attempt.
    /// Defaults to 12. With the default <see cref="RetryDelay"/> that is about one minute.
    /// Zero means the first delivery with no handler is a failed attempt.
    /// </summary>
    public int MaxHandlerDeferrals { get; set; } = 12;

    /// <summary>
    /// A wait for a missing handler is logged as a warning once the deferral count reaches this value.
    /// </summary>
    public const int HandlerDeferralWarningThreshold = 3;

    /// <summary>
    /// When <c>true</c>, the filter lists only the event types this process recorded.
    /// Types left by an earlier deploy are removed. Defaults to <c>false</c>, so the filter only grows.
    /// The match for a missing <c>lawndart_type</c> stays in either case.
    /// </summary>
    public bool ReplaceRouteRule { get; set; }

    /// <summary>
    /// What to do when no SQS queue is subscribed to the topic.
    /// Defaults to <see cref="AwsSqsUnroutablePublish.Warn"/>.
    /// The call does not record a successful publish in either mode.
    /// </summary>
    public AwsSqsUnroutablePublish UnroutablePublish { get; set; } = AwsSqsUnroutablePublish.Warn;

    /// <summary>
    /// Checks the shared settings and the SQS settings.
    /// An empty region is valid when a client is registered or the AWS SDK default chain can resolve one.
    /// </summary>
    public override void EnsureValid()
    {
        base.EnsureValid();
        AwsSqsNames.RequireSubscription(SubscriptionName);

        if (RetryDelay <= TimeSpan.Zero || RetryDelay > TimeSpan.FromSeconds(MaxDelaySeconds))
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.RetryDelay must be greater than zero and at most 15 minutes.");
        }

        if (OperationTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.OperationTimeout must be greater than zero.");
        }

        if (LongPollWait < TimeSpan.Zero || LongPollWait > TimeSpan.FromSeconds(MaxLongPollSeconds))
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.LongPollWait must be from 0 to 20 seconds.");
        }

        if (VisibilityTimeout <= TimeSpan.Zero || VisibilityTimeout > TimeSpan.FromSeconds(MaxVisibilitySeconds))
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.VisibilityTimeout must be greater than zero and at most 12 hours.");
        }

        if (MaxHandlerDeferrals < 0)
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.MaxHandlerDeferrals must be zero or greater.");
        }

        if (MaxReceiveCount < 1 || MaxReceiveCount > MaxReceiveCountLimit)
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.MaxReceiveCount must be from 1 to 1000.");
        }

        if (!string.IsNullOrWhiteSpace(Region) && !IsRegion(Region))
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.Region must be a region code, such as us-east-1.");
        }

        if (!string.IsNullOrWhiteSpace(ServiceUrl) && !IsHttpUrl(ServiceUrl))
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.ServiceUrl must be an absolute http or https URL.");
        }

        if (!Enum.IsDefined(UnroutablePublish))
        {
            throw new InvalidOperationException(
                "AwsSqsTransportOptions.UnroutablePublish is not a known value.");
        }
    }

    internal static bool IsRegion(string value)
    {
        if (value.Contains("://", StringComparison.Ordinal) || value.Contains('/', StringComparison.Ordinal))
            return false;

        if (value.Any(char.IsWhiteSpace))
            return false;

        return value.Contains('-', StringComparison.Ordinal);
    }

    internal static bool IsHttpUrl(string value)
        => Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
           (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
}
