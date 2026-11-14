using Microsoft.Extensions.Hosting;

namespace LawnDart.EventSourcing.Outbox;

/// <summary>
/// Runs <see cref="OutboxProcessor"/> and drains it during host shutdown,
/// before other hosted services stop.
/// </summary>
internal sealed class OutboxHost : IHostedService, IHostedLifecycleService, IDisposable
{
    private readonly OutboxProcessor _processor;

    public OutboxHost(OutboxProcessor processor) => _processor = processor;

    /// <inheritdoc />
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
        => _processor.StartAsync(cancellationToken);

    /// <inheritdoc />
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        _processor.RequestStop();
        return _processor.WaitUntilIdleAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
        => _processor.StopAsync(cancellationToken);

    /// <inheritdoc />
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public void Dispose() => _processor.Dispose();
}
