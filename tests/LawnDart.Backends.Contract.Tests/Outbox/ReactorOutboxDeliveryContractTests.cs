using System.Text.Json;
using LawnDart;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using LawnDart.Outbox;
using LawnDart.Patterns.Reaction;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

public abstract class ReactorOutboxDeliveryContractTests : BackendContractSuite
{
    protected ReactorOutboxDeliveryContractTests(IFullContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public async Task Claim_held_by_an_unfinished_attempt_is_retried_and_the_handler_runs_once()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.HasInboxStore, "This backend does not have an inbox yet.");

        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated(inboxClaimLease: TimeSpan.FromHours(1)));
        var transport = NewTransport();
        var reactor = new CountingReactor();
        var service = await StartReactorAsync(reactor, transport, inbox.Store);
        try
        {
            var id = Guid.NewGuid();
            var key = InboxConsumerKey.ForReactor(typeof(CountingReactor), id.ToString());
            Assert.True(await inbox.Store.TryClaimAsync(key));
            await writer.WriteAsync(NewPing(id, "held"));

            await new BatchProcessor(writer, NewPublisher(transport), maxAttempts: 10).ProcessBatchAsync();

            Assert.Equal(0, reactor.CallCount);
            var held = Assert.Single(await writer.GetUnprocessedAsync(10));
            Assert.Equal(id, held.Id);
            Assert.Equal(1, held.Attempts);
            Assert.Null(held.ProcessedAt);
            Assert.Contains("claim", held.LastError, StringComparison.OrdinalIgnoreCase);

            await inbox.Store.ReleaseClaimAsync(key);
            await new BatchProcessor(writer, NewPublisher(transport), maxAttempts: 10).ProcessBatchAsync();

            Assert.Equal(1, reactor.CallCount);
            Assert.Empty(await writer.GetUnprocessedAsync(10));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [ContractFact]
    public async Task Already_processed_message_is_skipped_and_the_handler_does_not_run_again()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.HasInboxStore, "This backend does not have an inbox yet.");

        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        var transport = NewTransport();
        var reactor = new CountingReactor();
        var service = await StartReactorAsync(reactor, transport, inbox.Store);
        try
        {
            var id = Guid.NewGuid();
            await transport.PublishAsync(new PingEvent(Guid.NewGuid(), DateTime.UtcNow, "first"), new MessageContext
            {
                MessageId = id.ToString()
            });
            Assert.Equal(1, reactor.CallCount);

            await writer.WriteAsync(NewPing(id, "again"));
            await new BatchProcessor(writer, NewPublisher(transport), maxAttempts: 10).ProcessBatchAsync();

            Assert.Equal(1, reactor.CallCount);
            Assert.Empty(await writer.GetUnprocessedAsync(10));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [ContractFact]
    public async Task Publish_with_no_subscriber_leaves_the_row_unprocessed_until_a_subscriber_exists()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.HasInboxStore, "This backend does not have an inbox yet.");

        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        var transport = NewTransport();
        var id = Guid.NewGuid();
        await writer.WriteAsync(NewPing(id, "early"));

        await new BatchProcessor(writer, NewPublisher(transport), maxAttempts: 10).ProcessBatchAsync();

        var waiting = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(id, waiting.Id);
        Assert.Equal(1, waiting.Attempts);
        Assert.Null(waiting.ProcessedAt);
        Assert.Contains("subscriber", waiting.LastError, StringComparison.OrdinalIgnoreCase);

        var reactor = new CountingReactor();
        var service = await StartReactorAsync(reactor, transport, inbox.Store);
        try
        {
            await new BatchProcessor(writer, NewPublisher(transport), maxAttempts: 10).ProcessBatchAsync();
            Assert.Equal(1, reactor.CallCount);
            Assert.Empty(await writer.GetUnprocessedAsync(10));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [ContractFact]
    public async Task Scoped_services_differ_between_two_messages()
    {
        SkipUnless(Cap.HasInboxStore, "This backend does not have an inbox yet.");

        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        var log = new ScopeLog();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IInboxStore>(inbox.Store);
        builder.Services.AddSingleton(log);
        builder.Services.AddScoped<ScopeToken>();
        builder.Services.AddInMemoryMessaging();
        builder.Services.AddReactor<TokenReactor, PingEvent>();

        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var transport = host.Services.GetRequiredService<InMemoryMessageTransport>();
            await WaitForSubscriberAsync(transport);
            await transport.PublishAsync(new PingEvent(Guid.NewGuid(), DateTime.UtcNow, "one"), new MessageContext
            {
                MessageId = "scope-1"
            });
            await transport.PublishAsync(new PingEvent(Guid.NewGuid(), DateTime.UtcNow, "two"), new MessageContext
            {
                MessageId = "scope-2"
            });

            Assert.Equal(2, log.Tokens.Count);
            Assert.NotSame(log.Tokens[0], log.Tokens[1]);
            Assert.True(log.Tokens[0].Disposed);
            Assert.True(log.Tokens[1].Disposed);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static InMemoryMessageTransport NewTransport()
        => new(
            NullLogger<InMemoryMessageTransport>.Instance,
            Options.Create(new MessageTransportOptions { MaxDeliveryAttempts = 1 }));

    private static MessageTransportOutboxPublisher NewPublisher(InMemoryMessageTransport transport)
        => new(transport, [typeof(PingEvent)]);

    private static OutboxMessage NewPing(Guid id, string note)
    {
        var evt = new PingEvent(Guid.NewGuid(), DateTime.UtcNow, note);
        return new OutboxMessage
        {
            Id = id,
            EventType = MessageTypeNames.Require(typeof(PingEvent)),
            SchemaVersion = 1,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(evt, MessagingJson.Options),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "reactor-outbox",
            SequencePosition = 1
        };
    }

    private static async Task<ReactorHostedService<CountingReactor, PingEvent>> StartReactorAsync(
        CountingReactor reactor,
        InMemoryMessageTransport transport,
        IInboxStore inbox)
    {
        var service = new ReactorHostedService<CountingReactor, PingEvent>(
            reactor,
            transport,
            inbox,
            NullLogger<ReactorHostedService<CountingReactor, PingEvent>>.Instance);
        await service.StartAsync(CancellationToken.None);
        await WaitForSubscriberAsync(transport);
        return service;
    }

    private static async Task WaitForSubscriberAsync(InMemoryMessageTransport transport)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (transport.SubscriberCount<PingEvent>() < 1)
            await Task.Delay(20, cts.Token);
    }

    [EventTypeName("reactor-outbox.ping")]
    private sealed record PingEvent(Guid Id, DateTime Timestamp, string Note) : IEvent;

    private sealed class CountingReactor : IReactor<PingEvent>
    {
        public int CallCount { get; private set; }

        public Task<IEnumerable<ICommand>> ReactAsync(
            PingEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult<IEnumerable<ICommand>>([]);
        }
    }

    private sealed class ScopeLog
    {
        public List<ScopeToken> Tokens { get; } = [];
    }

    private sealed class ScopeToken : IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }

    private sealed class TokenReactor : IReactor<PingEvent>
    {
        private readonly ScopeToken _token;
        private readonly ScopeLog _log;

        public TokenReactor(ScopeToken token, ScopeLog log)
        {
            _token = token;
            _log = log;
        }

        public Task<IEnumerable<ICommand>> ReactAsync(
            PingEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            _log.Tokens.Add(_token);
            return Task.FromResult<IEnumerable<ICommand>>([]);
        }
    }

    private sealed class BatchProcessor : OutboxProcessor
    {
        public BatchProcessor(IOutboxWriter writer, IOutboxPublisher publisher, int maxAttempts)
            : base(writer, publisher, NullLogger<OutboxProcessor>.Instance, TimeSpan.FromSeconds(5), batchSize: 100, maxAttempts)
        {
        }

        public Task ProcessBatchAsync()
        {
            var method = typeof(OutboxProcessor).GetMethod(
                "ProcessBatchAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (Task)method!.Invoke(this, [CancellationToken.None])!;
        }
    }
}
