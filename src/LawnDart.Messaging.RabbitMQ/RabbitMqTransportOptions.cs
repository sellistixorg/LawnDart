namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Connection settings for <see cref="RabbitMqMessageTransport"/>.
/// Includes the shared subscription, prefetch, retry, and dead-letter settings.
/// </summary>
public sealed class RabbitMqTransportOptions : MessageTransportOptions
{
    /// <summary>
    /// AMQP URI, such as <c>amqp://rabbitmq:rabbitmq@localhost:5672/</c>.
    /// When this is set, host, port, user, password, and virtual host come from the URI.
    /// </summary>
    public string? Uri { get; set; }

    /// <summary>Broker host. Used when <see cref="Uri"/> is not set. Defaults to <c>localhost</c>.</summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>Broker port. Used when <see cref="Uri"/> is not set. Defaults to <c>5672</c>.</summary>
    public int Port { get; set; } = 5672;

    /// <summary>User name. Used when <see cref="Uri"/> is not set. Defaults to <c>guest</c>.</summary>
    public string UserName { get; set; } = "guest";

    /// <summary>Password. Used when <see cref="Uri"/> is not set. Defaults to <c>guest</c>.</summary>
    public string Password { get; set; } = "guest";

    /// <summary>Virtual host. Used when <see cref="Uri"/> is not set. Defaults to <c>/</c>.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// How long a failed delivery waits before it returns to the subscription queue.
    /// The wait is the TTL on that subscription's retry queue. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// What to do when a publish is mandatory and no queue is bound.
    /// Defaults to <see cref="RabbitMqUnroutablePublish.Warn"/>.
    /// </summary>
    public RabbitMqUnroutablePublish UnroutablePublish { get; set; } = RabbitMqUnroutablePublish.Warn;

    /// <summary>
    /// Checks the shared settings and the RabbitMQ connection settings.
    /// </summary>
    public override void EnsureValid()
    {
        if (Concurrency < 1 || Concurrency > ushort.MaxValue)
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.Concurrency must be from 1 to 65535.");
        }

        base.EnsureValid();

        if (Prefetch > ushort.MaxValue)
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.Prefetch must be 65535 or less.");
        }

        if (RetryDelay <= TimeSpan.Zero || RetryDelay.TotalMilliseconds > int.MaxValue)
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.RetryDelay must be greater than zero and at most 24.8 days.");
        }

        if (!string.IsNullOrWhiteSpace(Uri))
        {
            if (!System.Uri.TryCreate(Uri, UriKind.Absolute, out var parsed) ||
                parsed.Scheme is not ("amqp" or "amqps"))
            {
                throw new InvalidOperationException(
                    "RabbitMqTransportOptions.Uri must be an absolute amqp or amqps URI.");
            }

            return;
        }

        if (string.IsNullOrWhiteSpace(HostName))
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.HostName is required when Uri is not set.");
        }

        if (Port is < 1 or > 65535)
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.Port must be between 1 and 65535.");
        }

        if (string.IsNullOrWhiteSpace(UserName))
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.UserName is required when Uri is not set.");
        }

        if (string.IsNullOrWhiteSpace(VirtualHost))
        {
            throw new InvalidOperationException(
                "RabbitMqTransportOptions.VirtualHost is required when Uri is not set.");
        }
    }
}

/// <summary>What a mandatory publish does when the broker cannot route the message.</summary>
public enum RabbitMqUnroutablePublish
{
    /// <summary>Log a warning and return. The broker did not queue the message.</summary>
    Warn = 0,

    /// <summary>Throw when the broker returns the message.</summary>
    Throw = 1,
}
