using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.Tests.Hosting;

public class HostedServiceDeclaredNameTests
{
    [Fact]
    public async Task Reactor_start_logs_and_throws_when_the_event_has_no_declared_name()
    {
        var logger = new CaptureLogger<ReactorHostedService<BareReactor, BareEvent>>();
        var service = new ReactorHostedService<BareReactor, BareEvent>(
            new BareReactor(),
            new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance),
            new InMemoryInboxStore(Options.Create(new MessagingOptions())),
            logger);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken.None));

        Assert.Contains("EventTypeName", ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(BareEvent).FullName!, ex.Message, StringComparison.Ordinal);
        Assert.Contains("EventTypeName", logger.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Processor_start_logs_and_throws_when_the_event_has_no_declared_name()
    {
        var logger = new CaptureLogger<EventProcessorHostedService<BareProcessor, BareEvent>>();
        var service = new EventProcessorHostedService<BareProcessor, BareEvent>(
            new BareProcessor(),
            new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance),
            new InMemoryInboxStore(Options.Create(new MessagingOptions())),
            logger);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken.None));

        Assert.Contains("EventTypeName", ex.Message, StringComparison.Ordinal);
        Assert.Contains("EventTypeName", logger.Error, StringComparison.Ordinal);
    }

    private sealed record BareEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class BareReactor : IReactor<BareEvent>
    {
        public Task<IEnumerable<ICommand>> ReactAsync(
            BareEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ICommand>>([]);
    }

    private sealed class BareProcessor : IEventProcessor<BareEvent>
    {
        public Task<IEnumerable<IEvent>> ProcessAsync(
            BareEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<IEvent>>([]);
    }

    private sealed class CaptureLogger<T> : ILogger<T>
    {
        public string Error { get; private set; } = "";

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
            => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error)
                Error = formatter(state, exception) + " " + exception?.Message;
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}
