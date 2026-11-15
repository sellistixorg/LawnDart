using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Outbox;

/// <summary>
/// Runs <see cref="OutboxProcessor"/> and drains it during host shutdown,
/// before other hosted services stop.
/// </summary>
internal sealed class OutboxHost : IHostedService, IHostedLifecycleService, IDisposable
{
    private readonly OutboxProcessor _processor;
    private readonly ILogger _logger;

    public OutboxHost(OutboxProcessor processor, ILogger<OutboxHost> logger)
    {
        _processor = processor;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _processor.StartAsync(cancellationToken).ConfigureAwait(false);
        var execute = _processor.ExecuteTask;
        if (execute is null)
            return;

        _ = execute.ContinueWith(
            static (task, state) =>
            {
                var logger = (ILogger)state!;
                logger.LogCritical(task.Exception, "Outbox processor failed.");
            },
            _logger,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <inheritdoc />
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        if (_processor.ExecuteTask is null)
            return Task.CompletedTask;

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
