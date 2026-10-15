using Testcontainers.LocalStack;

namespace LawnDart.Messaging.AwsSqs.Tests;

public sealed class AwsSqsContainerFixture : IAsyncLifetime
{
    // 4.14 is the last LocalStack image that starts without an auth token.
    private readonly LocalStackContainer _container = new LocalStackBuilder("localstack/localstack:4.14.0").Build();

    public string ServiceUrl { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        Environment.SetEnvironmentVariable("AWS_ACCESS_KEY_ID", "test");
        Environment.SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "test");
        Environment.SetEnvironmentVariable("AWS_REGION", "us-east-1");
        Environment.SetEnvironmentVariable("AWS_DEFAULT_REGION", "us-east-1");
        await _container.StartAsync();
        ServiceUrl = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class AwsSqsCollection : ICollectionFixture<AwsSqsContainerFixture>
{
    public const string Name = "AwsSqs";
}
