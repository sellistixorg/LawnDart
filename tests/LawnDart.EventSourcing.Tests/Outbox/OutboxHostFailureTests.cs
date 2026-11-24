using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Tests.Outbox;

/// <summary>
/// A failed outbox loop follows the host's <see cref="BackgroundServiceExceptionBehavior"/>.
/// </summary>
public class OutboxHostFailureTests
{
    [Fact]
    public async Task Failed_outbox_loop_stops_the_host_by_default()
    {
        var log = new CapturingLog();
        using var host = BuildHost(log, behavior: null);
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await WaitForAsync(lifetime.ApplicationStopping).WaitAsync(TimeSpan.FromSeconds(10));
        await StopQuietlyAsync(host);

        Assert.Contains(
            log.Entries,
            entry => entry.Level == LogLevel.Critical &&
                     entry.Message.Contains("Outbox processor failed", StringComparison.Ordinal) &&
                     entry.Exception is InvalidOperationException { Message: "outbox loop broke" });
    }

    [Fact]
    public async Task Failed_outbox_loop_with_StopHost_stops_the_host()
    {
        var log = new CapturingLog();
        using var host = BuildHost(log, BackgroundServiceExceptionBehavior.StopHost);
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await WaitForAsync(lifetime.ApplicationStopping).WaitAsync(TimeSpan.FromSeconds(10));
        await StopQuietlyAsync(host);

        Assert.True(lifetime.ApplicationStopping.IsCancellationRequested);
    }

    [Fact]
    public async Task Failed_outbox_loop_with_Ignore_keeps_the_host_running_and_logs()
    {
        var log = new CapturingLog();
        using var host = BuildHost(log, BackgroundServiceExceptionBehavior.Ignore);
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            await WaitUntilAsync(() => log.Entries.Any(entry => entry.Level == LogLevel.Critical));
            await Task.Delay(200);
            Assert.False(lifetime.ApplicationStopping.IsCancellationRequested);
            Assert.Contains(
                log.Entries,
                entry => entry.Message.Contains("Outbox processor failed", StringComparison.Ordinal));
        }
        finally
        {
            await StopQuietlyAsync(host);
        }
    }

    [Fact]
    public async Task Normal_shutdown_is_not_treated_as_a_failure()
    {
        var log = new CapturingLog();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(log);
        builder.Services.Configure<LawnDart.LawnDartOptions>(options => options.RequireTenantId = false);
        builder.Services.AddSingleton<IOutboxPublisher, NoOpPublisher>();
        builder.Services.AddBoundedContext("default")
            .WithEventTypes(typeof(FailurePing))
            .PublishEvents(events => events.Publish<FailurePing>())
            .UseInMemory(options => options.EnableOutbox = true);
        using var host = builder.Build();
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        var outbox = Assert.Single(host.Services.GetServices<IHostedService>().OfType<OutboxHost>());
        Assert.IsAssignableFrom<BackgroundService>(outbox);

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await Task.Delay(100);
        Assert.False(lifetime.ApplicationStopping.IsCancellationRequested);
        await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.NotNull(outbox.ExecuteTask);
        Assert.True(outbox.ExecuteTask!.IsCompletedSuccessfully);
        Assert.DoesNotContain(log.Entries, entry => entry.Level >= LogLevel.Error);
    }

    private static IHost BuildHost(CapturingLog log, BackgroundServiceExceptionBehavior? behavior)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(log);
        if (behavior is { } value)
            builder.Services.Configure<HostOptions>(options => options.BackgroundServiceExceptionBehavior = value);
        builder.Services.AddSingleton<IHostedService>(sp => new OutboxHost(
            new FaultingProcessor(),
            sp.GetRequiredService<ILogger<OutboxHost>>()));
        return builder.Build();
    }

    private static async Task StopQuietlyAsync(IHost host)
    {
        try
        {
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (Exception ex) when (ex is AggregateException or InvalidOperationException)
        {
            // Some host versions report the failed background service again on stop.
        }
    }

    private static Task WaitForAsync(CancellationToken token)
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        token.Register(() => done.TrySetResult());
        return done.Task;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
            await Task.Delay(20, cts.Token);
    }

    /// <summary>A processor whose loop fails after it starts.</summary>
    private sealed class FaultingProcessor : OutboxProcessor
    {
        public FaultingProcessor()
            : base(new InMemoryOutboxWriter(), new NoOpPublisher(), NullLogger<OutboxProcessor>.Instance)
        {
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Yield();
            throw new InvalidOperationException("outbox loop broke");
        }
    }

    private sealed class NoOpPublisher : IOutboxPublisher
    {
        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

    private sealed class CapturingLog : ILoggerProvider, ILogger
    {
        private readonly List<LogEntry> _entries = [];

        public IReadOnlyList<LogEntry> Entries
        {
            get
            {
                lock (_entries)
                    return _entries.ToArray();
            }
        }

        public ILogger CreateLogger(string categoryName) => this;

        public void Dispose()
        {
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;
            lock (_entries)
                _entries.Add(new LogEntry(logLevel, formatter(state, exception), exception));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }

    [EventTypeName("outbox-failure.ping")]
    private sealed record FailurePing(Guid Id, DateTime Timestamp) : IEvent;
}
