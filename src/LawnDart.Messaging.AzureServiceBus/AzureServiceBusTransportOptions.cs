using Azure.Core;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Connection settings for <see cref="AzureServiceBusMessageTransport"/>.
/// Includes the shared subscription, prefetch, retry, and dead-letter settings.
/// </summary>
public sealed class AzureServiceBusTransportOptions : MessageTransportOptions
{
    /// <summary>
    /// Azure Service Bus connection string (<c>Endpoint=sb://...</c>).
    /// Set this, or set <see cref="FullyQualifiedNamespace"/> and <see cref="Credential"/>,
    /// or register a <c>ServiceBusClient</c>. One data-plane mode is enough.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Fully qualified namespace, such as <c>my-namespace.servicebus.windows.net</c>.
    /// Used with <see cref="Credential"/> when <see cref="ConnectionString"/> is not set.
    /// Pass the host name, not an <c>sb://</c> URI.
    /// </summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>
    /// Token credential for <see cref="FullyQualifiedNamespace"/>.
    /// A credential from Azure.Identity, such as <c>DefaultAzureCredential</c>, implements this type.
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// Connection string for the administration client that creates topics and subscriptions.
    /// When this is unset, <see cref="ConnectionString"/> or <see cref="FullyQualifiedNamespace"/> is used.
    /// The Service Bus emulator serves management on a different port than AMQP, so that host sets this.
    /// </summary>
    public string? AdministrationConnectionString { get; set; }

    /// <summary>
    /// How long a failed or cancelled delivery waits before it is delivered again.
    /// The wait is the scheduled enqueue time. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a Service Bus call and an internal wait may run before they time out.
    /// Defaults to 30 seconds.
    /// </summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How many times the broker may deliver one locked message before it dead-letters on its own.
    /// A scheduled retry completes the current delivery, so this is not
    /// <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
    /// Defaults to <see cref="BrokerMaxDeliveryCountLimit"/>, which Azure Service Bus accepts.
    /// The emulator accepts 1 to 10.
    /// </summary>
    public int BrokerMaxDeliveryCount { get; set; } = BrokerMaxDeliveryCountLimit;

    /// <summary>Highest <see cref="BrokerMaxDeliveryCount"/> Azure Service Bus accepts.</summary>
    public const int BrokerMaxDeliveryCountLimit = 2000;

    /// <summary>
    /// Checks the shared settings and the Service Bus connection settings.
    /// An empty connection is valid when a <c>ServiceBusClient</c> is registered separately.
    /// </summary>
    public override void EnsureValid()
    {
        base.EnsureValid();
        AzureServiceBusNames.RequireSubscription(SubscriptionName);

        if (RetryDelay <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.RetryDelay must be greater than zero.");
        }

        if (OperationTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.OperationTimeout must be greater than zero.");
        }

        if (BrokerMaxDeliveryCount < 1 || BrokerMaxDeliveryCount > BrokerMaxDeliveryCountLimit)
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.BrokerMaxDeliveryCount must be from 1 to 2000.");
        }

        var hasConnectionString = !string.IsNullOrWhiteSpace(ConnectionString);
        var hasNamespace = !string.IsNullOrWhiteSpace(FullyQualifiedNamespace);
        var hasCredential = Credential is not null;

        if (hasConnectionString && (hasNamespace || hasCredential))
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions accepts a connection string, or a fully qualified namespace and credential, not both.");
        }

        if (hasNamespace != hasCredential)
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.FullyQualifiedNamespace and Credential are set together.");
        }

        if (hasConnectionString && !IsServiceBusConnectionString(ConnectionString!))
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.ConnectionString must be an Azure Service Bus connection string (Endpoint=sb://...).");
        }

        if (hasNamespace && !IsNamespaceHost(FullyQualifiedNamespace!))
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.FullyQualifiedNamespace must be a host name, such as my-namespace.servicebus.windows.net.");
        }

        if (!string.IsNullOrWhiteSpace(AdministrationConnectionString) &&
            !IsServiceBusConnectionString(AdministrationConnectionString))
        {
            throw new InvalidOperationException(
                "AzureServiceBusTransportOptions.AdministrationConnectionString must be an Azure Service Bus connection string (Endpoint=sb://...).");
        }
    }

    internal bool HasDataConnection =>
        !string.IsNullOrWhiteSpace(ConnectionString) ||
        (!string.IsNullOrWhiteSpace(FullyQualifiedNamespace) && Credential is not null);

    internal bool HasAdminConnection =>
        !string.IsNullOrWhiteSpace(AdministrationConnectionString) || HasDataConnection;

    internal static bool IsServiceBusConnectionString(string value)
    {
        const string marker = "Endpoint=";
        var start = value.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (start < 0)
            return false;

        var rest = value[(start + marker.Length)..];
        var end = rest.IndexOf(';');
        var endpoint = end >= 0 ? rest[..end] : rest;
        return endpoint.StartsWith("sb://", StringComparison.OrdinalIgnoreCase);
    }

    internal static bool IsNamespaceHost(string value)
    {
        if (value.Contains("://", StringComparison.Ordinal) || value.Contains('/', StringComparison.Ordinal))
            return false;

        if (value.Any(char.IsWhiteSpace))
            return false;

        return value.Contains('.', StringComparison.Ordinal);
    }
}
