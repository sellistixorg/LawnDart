using System.Reflection;
using Amazon.SimpleNotificationService;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using LawnDart.Messaging.AwsSqs;
using LawnDart.Messaging.Outbox;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AwsSqs.Tests;

public class AwsSqsStartupCheckTests
{
    [Fact]
    public async Task No_topic_is_a_missing_listener()
    {
        var transport = Transport(topics: [], subscriptions: []);
        Assert.False(await HasListener(transport));
    }

    [Fact]
    public async Task A_live_queue_is_a_listener_and_is_remembered()
    {
        var topic = "arn:aws:sns:us-east-1:000000000000:sqs-startup-ping";
        var sns = Proxy();
        sns.Topics =
        [
            new Topic { TopicArn = topic }
        ];
        sns.Subscriptions =
        [
            new Subscription
            {
                Protocol = "sqs",
                SubscriptionArn = topic + ":receiver",
                Endpoint = "arn:aws:sqs:us-east-1:000000000000:receiver"
            }
        ];
        var transport = Transport(sns);
        Assert.True(await HasListener(transport));

        sns.Subscriptions = [];
        Assert.True(await HasListener(transport));
    }

    [Fact]
    public async Task A_list_failure_is_not_a_missing_listener()
    {
        var sns = Proxy();
        sns.ListTopicsError = new InvalidOperationException("access denied");
        var transport = Transport(sns);
        await Assert.ThrowsAsync<InvalidOperationException>(() => HasListener(transport));
    }

    [Fact]
    public async Task Throw_stops_startup_when_no_queue_is_subscribed()
    {
        var transport = Transport(topics: [], subscriptions: []);
        var logs = new ListLogger();
        var check = Check(transport, logs, MissingListenerBehavior.Throw);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartedAsync(CancellationToken.None));
        Assert.Contains("sqs-startup-ping", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Warn_logs_a_missing_queue_and_continues()
    {
        var transport = Transport(topics: [], subscriptions: []);
        var logs = new ListLogger();
        var check = Check(transport, logs, MissingListenerBehavior.Warn);

        await check.StartedAsync(CancellationToken.None);
        Assert.Contains(
            logs.Lines,
            line => line.Contains("sqs-startup-ping", StringComparison.Ordinal) &&
                    line.Contains("no listener", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_pending_local_subscription_counts_as_listened()
    {
        var transport = Transport(topics: [], subscriptions: []);
        Assert.False(await HasListener(transport, typeof(SqsPing)));
        Assert.False(await HasListener(transport, typeof(SqsPong)));

        foreach (var behavior in new[] { MissingListenerBehavior.Warn, MissingListenerBehavior.Throw })
        {
            var logs = new ListLogger();
            var check = Check(
                transport,
                logs,
                behavior,
                configure: services =>
                {
                    services.AddReactor<PingReactor, SqsPing>();
                    services.AddEventProcessor<PongProcessor, SqsPong>();
                },
                typeof(SqsPing),
                typeof(SqsPong));
            await check.StartedAsync(CancellationToken.None);
            Assert.DoesNotContain(logs.Lines, line => line.Contains("no listener", StringComparison.Ordinal));
            Assert.DoesNotContain(logs.Lines, line => line.Contains("Startup continues", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task Throw_continues_when_the_list_fails()
    {
        var sns = Proxy();
        sns.ListTopicsError = new InvalidOperationException("access denied");
        var logs = new ListLogger();
        var check = Check(Transport(sns), logs, MissingListenerBehavior.Throw);

        await check.StartedAsync(CancellationToken.None);
        Assert.Contains(
            logs.Entries,
            entry => entry.Level == LogLevel.Warning &&
                     entry.Line.Contains("sqs-startup-ping", StringComparison.Ordinal) &&
                     entry.Line.Contains("Startup continues", StringComparison.Ordinal));
    }

    private static Task<bool> HasListener(AwsSqsMessageTransport transport)
        => HasListener(transport, typeof(SqsPing));

    private static Task<bool> HasListener(AwsSqsMessageTransport transport, Type messageType)
        => ((IOutboundListenerProbe)transport).HasListenerAsync(messageType, CancellationToken.None);

    private static OutboundListenerCheck Check(
        AwsSqsMessageTransport transport,
        ListLogger logs,
        MissingListenerBehavior behavior,
        Action<IServiceCollection>? configure = null,
        params Type[] types)
    {
        var plan = new OutboundListenerPlan();
        plan.Add(types.Length > 0 ? types : [typeof(SqsPing)]);
        var services = new ServiceCollection();
        configure?.Invoke(services);
        return new OutboundListenerCheck(
            plan,
            transport,
            Options.Create(new MessageTransportOptions { OnMissingListenerAtStartup = behavior }),
            services.BuildServiceProvider(),
            logs);
    }

    private static AwsSqsMessageTransport Transport(
        IReadOnlyList<Topic> topics,
        IReadOnlyList<Subscription> subscriptions)
    {
        var sns = Proxy();
        sns.Topics = topics.ToList();
        sns.Subscriptions = subscriptions.ToList();
        return Transport(sns);
    }

    private static AwsSqsMessageTransport Transport(SnsProxy sns)
    {
        var options = Options.Create(new AwsSqsTransportOptions());
        IAmazonSimpleNotificationService client = (IAmazonSimpleNotificationService)(object)sns;
        var connector = new AwsSqsConnector(
            options.Value,
            () => DispatchProxy.Create<IAmazonSQS, UnusedProxy>(),
            () => client,
            ownsClients: false);
        return new AwsSqsMessageTransport(
            NullLogger<AwsSqsMessageTransport>.Instance,
            options,
            preHandlers: null,
            deadLetters: null,
            connector,
            ownsConnector: false);
    }

    private static SnsProxy Proxy()
    {
        var proxy = DispatchProxy.Create<IAmazonSimpleNotificationService, SnsProxy>();
        return (SnsProxy)(object)proxy;
    }

    [EventTypeName("sqs-startup-ping")]
    private sealed record SqsPing(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("sqs-startup-pong")]
    private sealed record SqsPong(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class PingReactor : IReactor<SqsPing>
    {
        public Task<IEnumerable<ICommand>> ReactAsync(
            SqsPing @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ICommand>>([]);
    }

    private sealed class PongProcessor : IEventProcessor<SqsPong>
    {
        public Task<IEnumerable<IEvent>> ProcessAsync(
            SqsPong @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<IEvent>>([]);
    }

    public class SnsProxy : DispatchProxy
    {
        public List<Topic> Topics { get; set; } = [];

        public List<Subscription> Subscriptions { get; set; } = [];

        public Exception? ListTopicsError { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is null)
                throw new InvalidOperationException("Missing SNS method.");
            if (targetMethod.Name == "Dispose")
                return null;
            if (targetMethod.Name == "ListTopicsAsync")
            {
                if (ListTopicsError is not null)
                    return Task.FromException<ListTopicsResponse>(ListTopicsError);
                return Task.FromResult(new ListTopicsResponse { Topics = Topics });
            }

            if (targetMethod.Name == "ListSubscriptionsByTopicAsync")
                return Task.FromResult(new ListSubscriptionsByTopicResponse { Subscriptions = Subscriptions });

            throw new InvalidOperationException($"Unexpected SNS call {targetMethod.Name}.");
        }
    }

    public class UnusedProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name == "Dispose")
                return null;
            throw new InvalidOperationException($"Unexpected SQS call {targetMethod?.Name}.");
        }
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
