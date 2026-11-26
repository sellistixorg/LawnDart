using System.Text.Json;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using LawnDart.Outbox;
using LawnDart.Patterns.Reaction;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Tests.Outbox;

public class OutboxHostOrderTests
{
    [Fact]
    public async Task Outbox_registered_before_the_reactor_delivers_once()
    {
        var reactor = new CountingReactor();
        var inner = new InMemoryOutboxWriter();
        var id = Guid.NewGuid();
        var marked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var signaling = new MarkSignalOutboxWriter(inner, id, marked);
        using var host = BuildHost(
            reactor,
            subscribe: true,
            configure: services => services.AddKeyedSingleton<IOutboxWriter>("default", signaling));
        await inner.WriteAsync(Message(id));

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            await marked.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(1, reactor.CallCount);
            var row = Assert.Single(inner.Snapshot());
            Assert.Equal(id, row.Id);
            Assert.Equal(0, row.Attempts);
            Assert.NotNull(row.ProcessedAt);
        }
        finally
        {
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task Shutdown_finishes_the_current_row_and_leaves_the_rest()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inside = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reactor = new CountingReactor();
        reactor.OnHandle = async transport =>
        {
            inside.TrySetResult();
            await release.Task;
            reactor.SubscribersWhenHandled = transport.SubscriberCount<PingEvent>();
        };
        using var host = BuildHost(reactor, subscribe: true);
        var transport = host.Services.GetRequiredService<InMemoryMessageTransport>();
        reactor.Transport = transport;
        var writer = Writer(host);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await writer.WriteAsync(Message(first, 1));
        await writer.WriteAsync(Message(second, 2));

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await inside.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var stop = host.StopAsync();
        Assert.False(stop.IsCompleted);
        Assert.Equal(1, transport.SubscriberCount<PingEvent>());

        release.TrySetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10));

        var rows = writer.Snapshot().OrderBy(row => row.SequencePosition).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(first, rows[0].Id);
        Assert.NotNull(rows[0].ProcessedAt);
        Assert.Equal(0, rows[0].Attempts);
        Assert.Equal(second, rows[1].Id);
        Assert.Null(rows[1].ProcessedAt);
        Assert.Equal(0, rows[1].Attempts);
        Assert.Null(rows[1].DeadLetteredAt);
        Assert.Equal(1, reactor.CallCount);
        Assert.Equal(1, reactor.SubscribersWhenHandled);
    }

    [Fact]
    public async Task Stop_after_a_failed_start_returns_without_a_timeout_warning()
    {
        var logs = new WarningLog();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Logging.AddProvider(logs);
        builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(5));
        builder.Services.AddHostedService<FailingStartup>();
        builder.Services.Configure<LawnDart.LawnDartOptions>(options => options.Tenancy = Tenancy.Single);
        builder.Services.AddInMemoryMessaging();
        builder.Services.AddMessageTransportOutboxPublisher(typeof(PingEvent));
        builder.Services.AddBoundedContext("default")
            .WithEventTypes(typeof(PingEvent))
            .PublishEvents(events => events.Publish<PingEvent>())
            .UseInMemory(options => options.EnableOutbox = true);
        using var host = builder.Build();

        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
        Assert.DoesNotContain(logs.Lines, line => line.Contains("still publishing", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Throw_on_a_missing_listener_stops_host_startup()
    {
        using var host = BuildHost(
            new CountingReactor(),
            subscribe: false,
            configure: services => services.Configure<MessageTransportOptions>(options =>
                options.OnMissingListenerAtStartup = MissingListenerBehavior.Throw));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.Contains("outbox-host.ping", ex.Message, StringComparison.Ordinal);
        Assert.Contains("no listener", ex.Message, StringComparison.Ordinal);
        await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Startup_warns_when_a_published_type_has_no_listener()
    {
        var warnings = new WarningLog();
        using var host = BuildHost(new CountingReactor(), subscribe: false, warnings);
        var writer = Writer(host);
        var id = Guid.NewGuid();
        await writer.WriteAsync(Message(id));
        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            await WaitUntilAsync(() => writer.Snapshot().Single(row => row.Id == id).Attempts >= 1);
            Assert.Contains(
                warnings.Lines,
                line => line.Contains("outbox-host.ping", StringComparison.Ordinal) &&
                        line.Contains("no listener", StringComparison.Ordinal));
        }
        finally
        {
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task Startup_does_not_warn_when_a_reactor_is_subscribed()
    {
        var reactor = new CountingReactor();
        var warnings = new WarningLog();
        using var host = BuildHost(reactor, subscribe: true, warnings);
        var writer = Writer(host);
        await writer.WriteAsync(Message(Guid.NewGuid()));
        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            await WaitUntilAsync(() => reactor.CallCount == 1);
            Assert.DoesNotContain(
                warnings.Lines,
                line => line.Contains("no listener", StringComparison.Ordinal));
        }
        finally
        {
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    private static IHost BuildHost(
        CountingReactor reactor,
        bool subscribe,
        WarningLog? warnings = null,
        Action<IServiceCollection>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        if (warnings is not null)
            builder.Logging.AddProvider(warnings);

        builder.Services.Configure<LawnDart.LawnDartOptions>(options => options.Tenancy = Tenancy.Single);
        builder.Services.AddInMemoryMessaging();
        builder.Services.AddMessageTransportOutboxPublisher(typeof(PingEvent));
        builder.Services.AddSingleton(reactor);
        builder.Services.AddBoundedContext("default")
            .WithEventTypes(typeof(PingEvent))
            .PublishEvents(events => events.Publish<PingEvent>())
            .UseInMemory(options => options.EnableOutbox = true);
        if (subscribe)
            builder.Services.AddReactor<CountingReactor, PingEvent>();
        configure?.Invoke(builder.Services);
        return builder.Build();
    }

    private static InMemoryOutboxWriter Writer(IHost host)
        => (InMemoryOutboxWriter)host.Services.GetRequiredKeyedService<IOutboxWriter>("default");

    private static OutboxMessage Message(Guid id, long sequence = 1)
    {
        var evt = new PingEvent(Guid.NewGuid(), DateTime.UtcNow);
        return new OutboxMessage
        {
            Id = id,
            EventType = MessageTypeNames.Require(typeof(PingEvent)),
            SchemaVersion = 1,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(evt, MessagingJson.Options),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "outbox:host",
            SequencePosition = sequence
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
            await Task.Delay(20, cts.Token);
    }

    private sealed class FailingStartup : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
            => throw new InvalidOperationException("startup failed");

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [EventTypeName("outbox-host.ping")]
    private sealed record PingEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CountingReactor : IReactor<PingEvent>
    {
        private int _calls;

        public InMemoryMessageTransport? Transport { get; set; }

        public int CallCount => Volatile.Read(ref _calls);

        public int SubscribersWhenHandled { get; set; }

        public Func<InMemoryMessageTransport, Task>? OnHandle { get; set; }

        public async Task<IEnumerable<ICommand>> ReactAsync(
            PingEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _calls);
            if (OnHandle is not null && Transport is not null)
                await OnHandle(Transport);
            return [];
        }
    }

    private sealed class WarningLog : ILoggerProvider, ILogger
    {
        private readonly List<string> _lines = [];

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_lines)
                    return _lines.ToArray();
            }
        }

        public ILogger CreateLogger(string categoryName) => this;

        public void Dispose()
        {
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => Empty.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel < LogLevel.Warning)
                return;
            lock (_lines)
                _lines.Add(formatter(state, exception));
        }

        private sealed class Empty : IDisposable
        {
            public static readonly Empty Instance = new();

            public void Dispose()
            {
            }
        }
    }

    private sealed class MarkSignalOutboxWriter(
        InMemoryOutboxWriter inner,
        Guid targetId,
        TaskCompletionSource marked) : IOutboxWriter
    {
        public Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
            => inner.InitializeSchemaAsync(cancellationToken);

        public Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
            => inner.WriteAsync(message, cancellationToken);

        public Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
            => inner.WriteBatchAsync(messages, cancellationToken);

        public Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(
            int batchSize, CancellationToken cancellationToken = default)
            => inner.GetUnprocessedAsync(batchSize, cancellationToken);

        public async Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            await inner.MarkAsProcessedAsync(messageId, cancellationToken).ConfigureAwait(false);
            if (messageId == targetId)
                marked.TrySetResult();
        }

        public Task RecordFailureAsync(
            Guid messageId, string error, long publishedTo = 0, CancellationToken cancellationToken = default)
            => inner.RecordFailureAsync(messageId, error, publishedTo, cancellationToken);

        public Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
            => inner.MarkAsDeadLetteredAsync(messageId, cancellationToken);

        public Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(
            int batchSize, CancellationToken cancellationToken = default)
            => inner.GetDeadLetteredAsync(batchSize, cancellationToken);

        public Task<bool> ResetDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
            => inner.ResetDeadLetteredAsync(messageId, cancellationToken);

        public Task<int> ResetAllDeadLetteredAsync(CancellationToken cancellationToken = default)
            => inner.ResetAllDeadLetteredAsync(cancellationToken);
    }
}
