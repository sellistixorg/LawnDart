using Testcontainers.RabbitMq;

namespace LawnDart.Messaging.RabbitMQ.Tests;

public sealed class RabbitMqContainerFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4.1-alpine")
        .WithUsername("rabbitmq")
        .WithPassword("rabbitmq")
        .Build();

    public string AmqpUri { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        AmqpUri = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqContainerFixture>
{
    public const string Name = "RabbitMq";
}
