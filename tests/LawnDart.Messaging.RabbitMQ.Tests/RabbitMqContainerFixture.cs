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

    public async Task<string> RabbitMqCtlAsync(params string[] arguments)
    {
        var command = new List<string> { "rabbitmqctl" };
        command.AddRange(arguments);
        var result = await _container.ExecAsync(command);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"rabbitmqctl exited {result.ExitCode}.{Environment.NewLine}{result.Stdout}{Environment.NewLine}{result.Stderr}");
        }

        return result.Stdout;
    }
}

[CollectionDefinition(Name)]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqContainerFixture>
{
    public const string Name = "RabbitMq";
}
