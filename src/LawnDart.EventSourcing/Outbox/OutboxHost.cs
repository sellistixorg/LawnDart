using LawnDart.EventStore;
using LawnDart.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Outbox;

/// <summary>
/// Runs <see cref="OutboxProcessor"/> and drains it during host shutdown,
/// before other hosted services stop.
/// </summary>
/// <remarks>
/// This is a <see cref="BackgroundService"/> whose <see cref="BackgroundService.ExecuteTask"/>
/// is the processor loop. When the loop fails, the host applies
/// <c>HostOptions.BackgroundServiceExceptionBehavior</c>, the same as for any other
/// background service. The default stops the host.
/// </remarks>
internal sealed class OutboxHost : BackgroundService, IHostedLifecycleService
{
    private readonly OutboxProcessor _processor;
    private readonly ILogger _logger;

    public OutboxHost(OutboxProcessor processor, ILogger<OutboxHost> logger)
    {
        _processor = processor;
        _logger = logger;
    }

    /// <summary>
    /// Builds the outbox host for one context from the registered
    /// <see cref="IOutboxPublisher"/> services, in registration order.
    /// </summary>
    internal static OutboxHost Create(
        IServiceProvider services,
        string contextName,
        IOutboxWriter writer,
        NewEventsSignal? newEvents)
    {
        var publishers = services.GetServices<IOutboxPublisher>().ToArray();
        if (publishers.Length == 0)
        {
            throw new InvalidOperationException(
                $"EnableOutbox is true for context '{contextName}' but no IOutboxPublisher is registered.");
        }

        if (publishers.Length > OutboxProcessor.MaxPublishers)
        {
            throw new InvalidOperationException(
                $"Context '{contextName}' has {publishers.Length} IOutboxPublisher registrations. " +
                $"The outbox supports at most {OutboxProcessor.MaxPublishers}.");
        }

        return new OutboxHost(
            new OutboxProcessor(
                writer,
                publishers,
                services.GetService<ILogger<OutboxProcessor>>() ?? NullLogger<OutboxProcessor>.Instance,
                services.GetService<IHostApplicationLifetime>(),
                newEvents: newEvents),
            services.GetService<ILogger<OutboxHost>>() ?? NullLogger<OutboxHost>.Instance);
    }

    /// <inheritdoc />
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await _processor.StartAsync(cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Completes when the processor loop ends. A failed loop is logged and rethrown
    /// so the host applies its background service exception behavior.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var execute = _processor.ExecuteTask;
        if (execute is null)
            return;

        try
        {
            await execute.ConfigureAwait(false);
        }
        catch (Exception ex) when (!execute.IsCanceled)
        {
            _logger.LogCritical(
                ex,
                "Outbox processor failed. Outbox rows are no longer published. The host applies its BackgroundServiceExceptionBehavior.");
            throw;
        }
    }

    /// <inheritdoc />
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        // A loop that never started, or already ended (for example after a failure), has nothing to drain.
        if (_processor.ExecuteTask is null or { IsCompleted: true })
            return Task.CompletedTask;

        _processor.RequestStop();
        return _processor.WaitUntilIdleAsync(cancellationToken);
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _processor.StopAsync(cancellationToken).ConfigureAwait(false);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public override void Dispose()
    {
        _processor.Dispose();
        base.Dispose();
    }
}
