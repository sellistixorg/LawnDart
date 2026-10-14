using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Builds the data-plane and administration clients from <see cref="AzureServiceBusTransportOptions"/>.
/// </summary>
internal static class AzureServiceBusClients
{
    public static ServiceBusClient CreateClient(AzureServiceBusTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var clientOptions = new ServiceBusClientOptions();
        clientOptions.RetryOptions.TryTimeout = options.OperationTimeout;

        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
            return new ServiceBusClient(options.ConnectionString, clientOptions);

        return new ServiceBusClient(options.FullyQualifiedNamespace, options.Credential, clientOptions);
    }

    public static ServiceBusAdministrationClient CreateAdmin(AzureServiceBusTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var adminOptions = new ServiceBusAdministrationClientOptions();
        adminOptions.Retry.NetworkTimeout = options.OperationTimeout;

        if (!string.IsNullOrWhiteSpace(options.AdministrationConnectionString))
            return new ServiceBusAdministrationClient(options.AdministrationConnectionString, adminOptions);

        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
            return new ServiceBusAdministrationClient(options.ConnectionString, adminOptions);

        return new ServiceBusAdministrationClient(options.FullyQualifiedNamespace, options.Credential, adminOptions);
    }
}
