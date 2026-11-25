using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.Outbox;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.Tests.Outbox;

public class OutboundListenerCheckTests
{
    [Fact]
    public async Task Warn_logs_each_missing_listener_and_continues()
    {
        var logs = new ListLogger();
        var probe = new ScriptedProbe();
        probe.Result[typeof(FirstEvent)] = false;
        probe.Result[typeof(SecondEvent)] = true;
        var check = Check(probe, logs, MissingListenerBehavior.Warn, typeof(FirstEvent), typeof(SecondEvent));

        await check.StartedAsync(CancellationToken.None);

        Assert.Contains(logs.Lines, line => line.Contains("first.event", StringComparison.Ordinal) && line.Contains("no listener", StringComparison.Ordinal));
        Assert.DoesNotContain(logs.Lines, line => line.Contains("second.event", StringComparison.Ordinal) && line.Contains("no listener", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Throw_stops_when_a_declared_type_has_no_listener()
    {
        var probe = new ScriptedProbe();
        probe.Result[typeof(FirstEvent)] = false;
        var check = Check(probe, new ListLogger(), MissingListenerBehavior.Throw, typeof(FirstEvent));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartedAsync(CancellationToken.None));
        Assert.Contains("first.event", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Throw_continues_when_the_check_cannot_tell()
    {
        var logs = new ListLogger();
        var probe = new ScriptedProbe();
        probe.Fail.Add(typeof(FirstEvent));
        var check = Check(probe, logs, MissingListenerBehavior.Throw, typeof(FirstEvent));

        await check.StartedAsync(CancellationToken.None);

        Assert.Contains(
            logs.Entries,
            entry => entry.Level == LogLevel.Warning &&
                     entry.Line.Contains("first.event", StringComparison.Ordinal) &&
                     entry.Line.Contains("Startup continues", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_failed_check_does_not_skip_a_later_missing_listener()
    {
        var logs = new ListLogger();
        var probe = new ScriptedProbe();
        probe.Fail.Add(typeof(FirstEvent));
        probe.Result[typeof(SecondEvent)] = false;
        var warn = Check(probe, logs, MissingListenerBehavior.Warn, typeof(FirstEvent), typeof(SecondEvent));

        await warn.StartedAsync(CancellationToken.None);
        Assert.Contains(logs.Lines, line => line.Contains("second.event", StringComparison.Ordinal) && line.Contains("no listener", StringComparison.Ordinal));

        var thrown = Check(probe, new ListLogger(), MissingListenerBehavior.Throw, typeof(FirstEvent), typeof(SecondEvent));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => thrown.StartedAsync(CancellationToken.None));
        Assert.Contains("second.event", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("first.event", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Throw_continues_when_the_transport_has_no_probe()
    {
        var logs = new ListLogger();
        var check = Check(new SilentTransport(), logs, MissingListenerBehavior.Throw, typeof(FirstEvent));

        await check.StartedAsync(CancellationToken.None);

        Assert.Contains(
            logs.Entries,
            entry => entry.Level == LogLevel.Warning &&
                     entry.Line.Contains("not available", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_second_registration_adds_its_types()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageTransport>(new ScriptedProbe());
        services.AddMessageTransportOutboxPublisher(typeof(FirstEvent));
        services.AddMessageTransportOutboxPublisher(typeof(SecondEvent));
        using var provider = services.BuildServiceProvider();
        var plan = provider.GetRequiredService<OutboundListenerPlan>();

        var types = plan.Resolve(provider);

        Assert.Contains(typeof(FirstEvent), types);
        Assert.Contains(typeof(SecondEvent), types);
    }

    [Fact]
    public async Task A_hand_registered_message_subscription_does_not_skip_the_check()
    {
        var logs = new ListLogger();
        var probe = new ScriptedProbe();
        probe.Result[typeof(FirstEvent)] = false;

        var warn = Check(
            probe,
            logs,
            MissingListenerBehavior.Warn,
            services => services.AddSingleton(new MessageSubscription(typeof(FirstEvent))),
            typeof(FirstEvent));
        await warn.StartedAsync(CancellationToken.None);

        Assert.Contains(
            logs.Lines,
            line => line.Contains("first.event", StringComparison.Ordinal) &&
                    line.Contains("no listener", StringComparison.Ordinal));

        var thrown = Check(
            probe,
            new ListLogger(),
            MissingListenerBehavior.Throw,
            services => services.AddSingleton(new MessageSubscription(typeof(FirstEvent))),
            typeof(FirstEvent));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => thrown.StartedAsync(CancellationToken.None));
        Assert.Contains("first.event", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Reactor_and_event_processor_registrations_skip_the_probe()
    {
        var probe = new ScriptedProbe();
        probe.Fail.Add(typeof(FirstEvent));
        probe.Fail.Add(typeof(SecondEvent));

        foreach (var behavior in new[] { MissingListenerBehavior.Warn, MissingListenerBehavior.Throw })
        {
            var logs = new ListLogger();
            var check = Check(
                probe,
                logs,
                behavior,
                services =>
                {
                    services.AddReactor<FirstReactor, FirstEvent>();
                    services.AddEventProcessor<SecondProcessor, SecondEvent>();
                },
                typeof(FirstEvent),
                typeof(SecondEvent));

            await check.StartedAsync(CancellationToken.None);
            Assert.DoesNotContain(logs.Lines, line => line.Contains("no listener", StringComparison.Ordinal));
            Assert.DoesNotContain(logs.Lines, line => line.Contains("Startup continues", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task A_hand_registered_subscription_still_fails_when_another_type_has_a_reactor()
    {
        var probe = new ScriptedProbe();
        probe.Result[typeof(FirstEvent)] = false;
        probe.Result[typeof(SecondEvent)] = false;

        var check = Check(
            probe,
            new ListLogger(),
            MissingListenerBehavior.Throw,
            services =>
            {
                services.AddReactor<FirstReactor, FirstEvent>();
                services.AddSingleton(new MessageSubscription(typeof(SecondEvent)));
            },
            typeof(FirstEvent),
            typeof(SecondEvent));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartedAsync(CancellationToken.None));
        Assert.Contains("second.event", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("first.event", ex.Message, StringComparison.Ordinal);
    }

    private static OutboundListenerCheck Check(
        IMessageTransport transport,
        ListLogger logs,
        MissingListenerBehavior behavior,
        params Type[] types)
        => Check(transport, logs, behavior, configure: null, types);

    private static OutboundListenerCheck Check(
        IMessageTransport transport,
        ListLogger logs,
        MissingListenerBehavior behavior,
        Action<IServiceCollection>? configure,
        params Type[] types)
    {
        var plan = new OutboundListenerPlan();
        plan.Add(types);
        var services = new ServiceCollection();
        configure?.Invoke(services);
        return new OutboundListenerCheck(
            plan,
            transport,
            Options.Create(new MessageTransportOptions { OnMissingListenerAtStartup = behavior }),
            services.BuildServiceProvider(),
            logs);
    }

    [EventTypeName("first.event")]
    private sealed record FirstEvent(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("second.event")]
    private sealed record SecondEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class FirstReactor : IReactor<FirstEvent>
    {
        public Task<IEnumerable<ICommand>> ReactAsync(
            FirstEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ICommand>>([]);
    }

    private sealed class SecondProcessor : IEventProcessor<SecondEvent>
    {
        public Task<IEnumerable<IEvent>> ProcessAsync(
            SecondEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<IEvent>>([]);
    }

    private sealed class ScriptedProbe : IMessageTransport, IOutboundListenerProbe
    {
        public Dictionary<Type, bool> Result { get; } = [];

        public HashSet<Type> Fail { get; } = [];

        public Task PublishAsync<TMessage>(TMessage message, MessageContext context, CancellationToken cancellationToken = default)
            where TMessage : class
            => Task.CompletedTask;

        public Task SubscribeAsync<TMessage>(
            Func<TMessage, MessageContext, CancellationToken, Task> handler,
            CancellationToken cancellationToken = default)
            where TMessage : class
            => Task.CompletedTask;

        public Task<bool> HasListenerAsync(Type messageType, CancellationToken cancellationToken)
        {
            if (Fail.Contains(messageType))
                throw new InvalidOperationException("cannot tell");
            return Task.FromResult(Result.TryGetValue(messageType, out var listening) && listening);
        }
    }

    private sealed class SilentTransport : IMessageTransport
    {
        public Task PublishAsync<TMessage>(TMessage message, MessageContext context, CancellationToken cancellationToken = default)
            where TMessage : class
            => Task.CompletedTask;

        public Task SubscribeAsync<TMessage>(
            Func<TMessage, MessageContext, CancellationToken, Task> handler,
            CancellationToken cancellationToken = default)
            where TMessage : class
            => Task.CompletedTask;
    }

    private sealed class ListLogger : ILogger<OutboundListenerCheck>
    {
        private readonly List<(LogLevel Level, string Line)> _entries = [];

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_entries)
                    return _entries.Select(entry => entry.Line).ToArray();
            }
        }

        public IReadOnlyList<(LogLevel Level, string Line)> Entries
        {
            get
            {
                lock (_entries)
                    return _entries.ToArray();
            }
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => Empty.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_entries)
                _entries.Add((logLevel, formatter(state, exception)));
        }

        private sealed class Empty : IDisposable
        {
            public static readonly Empty Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
