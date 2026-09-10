using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Telemetry;
using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Hosted service that drives a single <see cref="ITaskProcessor"/> on a configurable polling interval.
/// Each poll calls <see cref="ITaskProcessor.ProcessTasksAsync"/>, then dispatches any returned
/// commands via <see cref="ICommandDispatcher"/> (if registered).
/// Implements exponential backoff when consecutive errors exceed the configured threshold.
/// </summary>
/// <typeparam name="TProcessor">The task processor implementation type.</typeparam>
public sealed class TaskProcessorHostedService<TProcessor> : BackgroundService
    where TProcessor : class, ITaskProcessor
{
    private readonly TProcessor _processor;
    private readonly TaskProcessorOptions _options;
    private readonly ICommandDispatcher? _commandDispatcher;
    private readonly ILogger<TaskProcessorHostedService<TProcessor>> _logger;

    private static readonly string ProcessorTypeName = typeof(TProcessor).Name;

    public TaskProcessorHostedService(
        TProcessor processor,
        IOptions<TaskProcessorOptions> options,
        ILogger<TaskProcessorHostedService<TProcessor>> logger,
        ICommandDispatcher? commandDispatcher = null)
    {
        _processor = processor;
        _options = options.Value;
        _commandDispatcher = commandDispatcher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int consecutiveErrors = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            using var activity = TaskProcessorTelemetry.StartPoll(ProcessorTypeName);
            var start = TimeProvider.System.GetTimestamp();

            try
            {
                var commands = (await _processor.ProcessTasksAsync(stoppingToken)).ToList();

                if (_commandDispatcher is not null)
                {
                    foreach (var command in commands)
                    {
                        var inbound = new MessageContext
                        {
                            MessageId = Guid.NewGuid().ToString(),
                            CorrelationId = activity?.TraceId is { } tid && tid != default
                                ? tid.ToHexString()
                                : null,
                            Headers = MessageTrace.WithCurrentTraceHeaders(null, activity)
                        };
                        await _commandDispatcher.DispatchAsync(command, inbound, stoppingToken);
                    }
                }
                else if (commands.Count > 0)
                {
                    _logger.LogWarning(
                        "TaskProcessor {Processor} emitted {Count} command(s) but no ICommandDispatcher is registered.",
                        ProcessorTypeName, commands.Count);
                }

                var elapsed = TimeProvider.System.GetElapsedTime(start);
                TaskProcessorTelemetry.RecordPoll(ProcessorTypeName, elapsed, commands.Count);

                consecutiveErrors = 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                consecutiveErrors++;
                TaskProcessorTelemetry.RecordError(ProcessorTypeName);
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);

                _logger.LogError(ex,
                    "TaskProcessor {Processor} poll failed (consecutive errors: {ErrorCount})",
                    ProcessorTypeName, consecutiveErrors);

                if (consecutiveErrors >= _options.ErrorBackoffThreshold)
                {
                    _logger.LogWarning(
                        "TaskProcessor {Processor} entering backoff for {BackoffDuration} after {ErrorCount} consecutive errors",
                        ProcessorTypeName, _options.ErrorBackoffDuration, consecutiveErrors);

                    await Task.Delay(_options.ErrorBackoffDuration, stoppingToken);
                    consecutiveErrors = 0;
                    continue;
                }
            }

            await Task.Delay(_options.PollingInterval, stoppingToken);
        }
    }
}
