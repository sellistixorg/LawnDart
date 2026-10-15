using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Builds the SQS and SNS clients from <see cref="AwsSqsTransportOptions"/>.
/// Credentials come from the AWS SDK default chain.
/// </summary>
internal static class AwsSqsClients
{
    public static IAmazonSQS CreateSqs(AwsSqsTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        try
        {
            return new AmazonSQSClient(CreateSqsConfig(options));
        }
        catch (AmazonClientException ex)
        {
            throw ClientFailure("Amazon SQS", ex);
        }
    }

    public static IAmazonSimpleNotificationService CreateSns(AwsSqsTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        try
        {
            return new AmazonSimpleNotificationServiceClient(CreateSnsConfig(options));
        }
        catch (AmazonClientException ex)
        {
            throw ClientFailure("Amazon SNS", ex);
        }
    }

    public static AmazonSQSConfig CreateSqsConfig(AwsSqsTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var config = new AmazonSQSConfig();
        Apply(config, options);
        return config;
    }

    public static AmazonSimpleNotificationServiceConfig CreateSnsConfig(AwsSqsTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var config = new AmazonSimpleNotificationServiceConfig();
        Apply(config, options);
        return config;
    }

    private static void Apply(ClientConfig config, AwsSqsTransportOptions options)
    {
        config.Timeout = options.OperationTimeout;
        config.MaxErrorRetry = 1;

        if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
        {
            config.ServiceURL = options.ServiceUrl;
            config.AuthenticationRegion = string.IsNullOrWhiteSpace(options.Region) ? "us-east-1" : options.Region;
            return;
        }

        if (!string.IsNullOrWhiteSpace(options.Region))
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);
    }

    private static InvalidOperationException ClientFailure(string kind, Exception exception)
        => new(
            $"Could not create an {kind} client. Set AwsSqsTransportOptions.Region or ServiceUrl, or register the client. Credentials come from the AWS SDK default chain.",
            exception);
}
