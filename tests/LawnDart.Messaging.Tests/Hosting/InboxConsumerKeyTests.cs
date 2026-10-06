using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.Reaction;
using LongInboxArg = LawnDart.Messaging.Tests.Hosting.LongInboxArgXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;

namespace LawnDart.Messaging.Tests.Hosting;

public class InboxConsumerKeyTests
{
    private const string MessageId = "11111111-1111-1111-1111-111111111111";

    [Fact]
    public void Generic_long_named_reactor_key_fits_sql_inbox_limit()
    {
        var type = typeof(GenericReactor<LongInboxArg>);
        var raw = "reactor:" + type.FullName + ":" + MessageId;
        Assert.True(raw.Length > InboxConsumerKey.MaxKeyLength);

        var key = InboxConsumerKey.ForReactor(type, MessageId);

        Assert.InRange(key.Length, 1, InboxConsumerKey.MaxKeyLength);
        Assert.Equal(ExpectedKey("reactor", type, MessageId), key);
        Assert.DoesNotContain("LongInboxArg", key, StringComparison.Ordinal);
    }

    [Fact]
    public void Distinct_generic_arguments_do_not_share_a_reactor_key()
    {
        var left = InboxConsumerKey.ForReactor(
            typeof(GenericReactor<LongInboxArg>),
            MessageId);
        var right = InboxConsumerKey.ForReactor(
            typeof(GenericReactor<OtherLongInboxArg>),
            MessageId);

        Assert.NotEqual(left, right);
        Assert.InRange(right.Length, 1, InboxConsumerKey.MaxKeyLength);
    }

    [Fact]
    public void Processor_key_uses_the_processor_prefix_and_the_same_hash()
    {
        var type = typeof(GenericReactor<LongInboxArg>);
        var key = InboxConsumerKey.ForProcessor(type, MessageId);

        Assert.Equal(ExpectedKey("processor", type, MessageId), key);
        Assert.NotEqual(InboxConsumerKey.ForReactor(type, MessageId), key);
    }

    [Fact]
    public void Oversized_message_id_is_hashed_so_the_key_still_fits()
    {
        var messageId = new string('m', InboxConsumerKey.MaxKeyLength);
        var type = typeof(GenericReactor<LongInboxArg>);
        var key = InboxConsumerKey.ForReactor(type, messageId);

        Assert.InRange(key.Length, 1, InboxConsumerKey.MaxKeyLength);
        Assert.DoesNotContain(messageId, key, StringComparison.Ordinal);
        Assert.Equal(
            "reactor:" + Hash(type.FullName!) + ":" + Hash(messageId),
            key);
    }

    [Fact]
    public async Task Hosted_reactor_with_a_long_generic_name_stores_a_short_key()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new RecordingInbox();
        var reactor = new GenericReactor<LongInboxArg>();

        using var cts = new CancellationTokenSource();
        var service = new ReactorHostedService<
            GenericReactor<LongInboxArg>,
            OrderPlacedEvent>(
            reactor,
            transport,
            inbox,
            NullLogger<ReactorHostedService<
                GenericReactor<LongInboxArg>,
                OrderPlacedEvent>>.Instance);

        await service.StartAsync(cts.Token);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (transport.SubscriberCount<OrderPlacedEvent>() < 1)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Reactor did not subscribe.");
            await Task.Delay(10);
        }

        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-1");
        await transport.PublishAsync(evt, new MessageContext { MessageId = MessageId });

        var expected = InboxConsumerKey.ForReactor(reactor.GetType(), MessageId);
        Assert.Contains(expected, inbox.Keys);
        Assert.All(inbox.Keys, key => Assert.InRange(key.Length, 1, InboxConsumerKey.MaxKeyLength));
        Assert.Equal(1, reactor.CallCount);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    private static string ExpectedKey(string role, Type type, string messageId)
        => role + ":" + Hash(type.FullName!) + ":" + messageId;

    private static string Hash(string value)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash.AsSpan(0, InboxConsumerKey.ConsumerHashLength / 2)).ToLowerInvariant();
    }

    private sealed class RecordingInbox : IInboxStore
    {
        public List<string> Keys { get; } = [];

        public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
        {
            Keys.Add(messageId);
            return Task.FromResult(false);
        }

        public Task MarkProcessedAsync(
            string messageId,
            DateTimeOffset processedAt,
            CancellationToken cancellationToken = default)
        {
            Keys.Add(messageId);
            return Task.CompletedTask;
        }
    }
}

/// <summary>Closed over a long type argument so the raw inbox key exceeds 256 characters.</summary>
internal sealed class GenericReactor<T> : IReactor<OrderPlacedEvent>
{
    public int CallCount { get; private set; }

    public Task<IEnumerable<ICommand>> ReactAsync(
        OrderPlacedEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        CallCount++;
        return Task.FromResult<IEnumerable<ICommand>>([]);
    }
}

internal sealed class LongInboxArgXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;
internal sealed class OtherLongInboxArg;
