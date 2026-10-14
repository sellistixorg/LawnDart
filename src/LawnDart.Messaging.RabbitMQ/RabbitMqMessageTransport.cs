using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using LawnDart.Messaging.Telemetry;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// RabbitMQ <see cref="IMessageTransport"/>.
/// The exchange and the routing key are the declared message type name.
/// Publish declares that exchange. A subscription declares its own queue.
/// A failed handler waits on a retry queue, then returns, until <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
/// When dead-letter handling is on, the last failure is nacked to the dead-letter exchange.
/// When it is off, the message waits and returns to the same queue.
/// </summary>
public sealed class RabbitMqMessageTransport : IMessageTransport, IDisposable, IAsyncDisposable
{
    /// <summary>Value stored on <see cref="MessageContext.TransportType"/>. The value is <c>RabbitMQ</c>.</summary>
    public const string TransportTypeName = "RabbitMQ";

    private readonly ILogger<RabbitMqMessageTransport> _logger;
    private readonly RabbitMqTransportOptions _options;
    private readonly IReadOnlyList<IMessagePreHandler> _preHandlers;
    private readonly IDeadLetterQueue _deadLetters;
    private readonly RabbitMqConnector _connector;
    private readonly bool _ownsConnector;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private readonly ConcurrentDictionary<Type, Subscription> _subscriptions = new();
    private IChannel? _publishChannel;
    private int _disposed;

    /// <summary>
    /// Creates a transport that opens its own connection.
    /// </summary>
    /// <param name="logger">Logger. A warning is written when a message is dead-lettered.</param>
    /// <param name="options">Connection, prefetch, retry, and dead-letter settings.</param>
    /// <param name="preHandlers">Hooks invoked before each handler attempt. May be empty.</param>
    /// <param name="deadLetters">In-process copy of messages parked on the dead-letter exchange. A memory queue is used when omitted.</param>
    public RabbitMqMessageTransport(
        ILogger<RabbitMqMessageTransport> logger,
        IOptions<RabbitMqTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers = null,
        IDeadLetterQueue? deadLetters = null)
        : this(logger, options, preHandlers, deadLetters, connector: null)
    {
    }

    internal RabbitMqMessageTransport(
        ILogger<RabbitMqMessageTransport> logger,
        IOptions<RabbitMqTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers,
        IDeadLetterQueue? deadLetters,
        RabbitMqConnector? connector)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger;
        _options = options.Value;
        _options.EnsureValid();
        _preHandlers = preHandlers?.ToArray() ?? [];
        _deadLetters = deadLetters ?? new MemoryDeadLetterQueue();
        _ownsConnector = connector is null;
        _connector = connector ?? new RabbitMqConnector(_options);
    }

    /// <summary>Messages parked after they used their delivery attempts.</summary>
    public IReadOnlyList<DeadLetteredMessage> DeadLetters => _deadLetters.ReadAll();

    /// <inheritdoc />
    public async Task PublishAsync<TMessage>(
        TMessage message,
        MessageContext context,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(context);

        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        cancellationToken.ThrowIfCancellationRequested();

        using var publish = MessagingTelemetry.StartPublish(declaredName);
        publish?.SetTag("messaging.rabbitmq.exchange", RabbitMqNames.Exchange(declaredName));
        publish?.SetTag("messaging.rabbitmq.routing_key", RabbitMqNames.RoutingKey(declaredName));
        var started = TimeProvider.System.GetTimestamp();

        try
        {
            await _connector.DeclareExchangeAsync(declaredName, cancellationToken).ConfigureAwait(false);

            var headers = new Dictionary<string, string>(StringComparer.Ordinal);
            MessageEnvelopeHeaders.Write(context, declaredName, headers);
            var table = new Dictionary<string, object?>(headers.Count, StringComparer.Ordinal);
            foreach (var pair in headers)
                table[pair.Key] = pair.Value;

            var properties = new BasicProperties
            {
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                MessageId = context.MessageId,
                CorrelationId = context.CorrelationId,
                Type = declaredName,
                Headers = table,
            };

            var body = JsonSerializer.SerializeToUtf8Bytes(message, MessagingJson.Options);
            var channel = await PublishChannelAsync(cancellationToken).ConfigureAwait(false);

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                try
                {
                    await channel.BasicPublishAsync(
                        RabbitMqNames.Exchange(declaredName),
                        RabbitMqNames.RoutingKey(declaredName),
                        mandatory: true,
                        properties,
                        body,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (PublishException ex) when (IsUnroutable(ex) &&
                    _options.UnroutablePublish == RabbitMqUnroutablePublish.Warn)
                {
                    _logger.LogWarning(
                        ex,
                        "No queue is bound for {MessageType}. The message was not queued. Exchange {Exchange}, routing key {RoutingKey}, message {MessageId}.",
                        declaredName,
                        RabbitMqNames.Exchange(declaredName),
                        RabbitMqNames.RoutingKey(declaredName),
                        context.MessageId);
                }
            }
            finally
            {
                _publishLock.Release();
            }

            MessagingTelemetry.RecordPublish(declaredName, TimeProvider.System.GetElapsedTime(started));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessagingTelemetry.RecordPublishError(declaredName);
            publish?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <inheritdoc />
    public Task SubscribeAsync<TMessage>(
        Func<TMessage, MessageContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        var subscription = _subscriptions.GetOrAdd(
            typeof(TMessage),
            _ => new Subscription(
                declaredName,
                RabbitMqNames.Queue(declaredName, _options.SubscriptionName),
                _options.Concurrency));

        var registration = new HandlerRegistration(handler, cancellationToken);
        lock (subscription.Handlers)
            subscription.Handlers.Add(registration);

        // The queue is bound before this method returns a task, so the next publish can route.
        EnsureConsumerAsync<TMessage>(subscription).GetAwaiter().GetResult();

        if (!cancellationToken.CanBeCanceled)
            return Task.CompletedTask;

        var unregister = cancellationToken.Register(() =>
        {
            lock (subscription.Handlers)
                subscription.Handlers.Remove(registration);
            registration.Completed.TrySetResult();
            _ = StopIfIdleAsync(subscription);
        });

        return WaitUntilCancelledAsync(Task.CompletedTask, registration, unregister);
    }

    /// <summary>
    /// Drops the handlers for <typeparamref name="TMessage"/> and leaves the consumer running.
    /// A delivery then waits on the retry queue instead of calling a handler.
    /// </summary>
    /// <typeparam name="TMessage">The message type whose handlers to drop.</typeparam>
    internal void RemoveHandlers<TMessage>()
        where TMessage : class
    {
        if (!_subscriptions.TryGetValue(typeof(TMessage), out var subscription))
            return;

        lock (subscription.Handlers)
            subscription.Handlers.Clear();
    }

    /// <summary>Closes consumers and the connection this transport opened.</summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>Closes consumers and the connection this transport opened.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var subscription in _subscriptions.Values)
        {
            List<HandlerRegistration> handlers;
            lock (subscription.Handlers)
            {
                handlers = [.. subscription.Handlers];
                subscription.Handlers.Clear();
            }

            foreach (var handler in handlers)
                handler.Completed.TrySetResult();

            await CloseChannelAsync(subscription.Channel).ConfigureAwait(false);
            subscription.Channel = null;
        }

        await _publishLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await CloseChannelAsync(_publishChannel).ConfigureAwait(false);
            _publishChannel = null;
        }
        finally
        {
            _publishLock.Release();
        }

        if (_ownsConnector)
            await _connector.DisposeAsync().ConfigureAwait(false);
    }

    private async Task EnsureConsumerAsync<TMessage>(Subscription subscription)
        where TMessage : class
    {
        Task startup;
        lock (subscription)
        {
            subscription.Startup ??= StartConsumerAsync<TMessage>(subscription);
            startup = subscription.Startup;
        }

        try
        {
            await startup.ConfigureAwait(false);
        }
        catch
        {
            lock (subscription)
            {
                if (ReferenceEquals(subscription.Startup, startup))
                    subscription.Startup = null;
            }

            throw;
        }
    }

    private async Task StartConsumerAsync<TMessage>(Subscription subscription)
        where TMessage : class
    {
        await _connector.DeclareSubscriptionAsync(subscription.DeclaredName, _options.SubscriptionName, CancellationToken.None)
            .ConfigureAwait(false);

        var concurrency = checked((ushort)_options.Concurrency);
        var channel = await _connector.CreateChannelAsync(
            new CreateChannelOptions(false, false, null, concurrency),
            CancellationToken.None).ConfigureAwait(false);

        var prefetch = checked((ushort)_options.Prefetch);
        await channel.BasicQosAsync(0, prefetch, global: false, CancellationToken.None).ConfigureAwait(false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) =>
            HandleDeliveryAsync<TMessage>(subscription, channel, delivery);

        var tag = await channel.BasicConsumeAsync(
            subscription.Queue,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer,
            cancellationToken: CancellationToken.None).ConfigureAwait(false);

        subscription.Channel = channel;
        subscription.ConsumerTag = tag;

        var idle = false;
        lock (subscription.Handlers)
            idle = subscription.Handlers.Count == 0;

        if (idle)
            await StopIfIdleAsync(subscription).ConfigureAwait(false);
    }

    private async Task HandleDeliveryAsync<TMessage>(
        Subscription subscription,
        IChannel channel,
        BasicDeliverEventArgs delivery)
        where TMessage : class
    {
        var body = delivery.Body.ToArray();
        var rawHeaders = RabbitMqHeaders.Copy(delivery.BasicProperties.Headers);
        var completed = RabbitMqHeaders.CompletedAttempts(rawHeaders);
        var attempt = completed + 1;
        var envelope = MessageEnvelopeHeaders.Read(
            RabbitMqHeaders.ToEnvelope(rawHeaders),
            TransportTypeName);

        using var consume = MessagingTelemetry.StartConsume(subscription.DeclaredName, envelope.Context.MessageId);
        consume?.SetTag("messaging.rabbitmq.queue", subscription.Queue);
        consume?.SetTag("messaging.rabbitmq.redelivered", delivery.Redelivered || completed > 0);
        var started = TimeProvider.System.GetTimestamp();
        var settled = false;
        var acquired = false;

        try
        {
            await subscription.Concurrency.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            acquired = true;

            List<HandlerRegistration> handlers;
            lock (subscription.Handlers)
                handlers = [.. subscription.Handlers];

            if (handlers.Count == 0)
            {
                _logger.LogDebug(
                    "No handler is registered for {MessageType} on {Queue}. The message returns after {Delay}.",
                    subscription.DeclaredName,
                    subscription.Queue,
                    _options.RetryDelay);
                await DelayRedeliveryAsync(
                    channel,
                    delivery.DeliveryTag,
                    subscription,
                    body,
                    rawHeaders,
                    completedAttempts: Math.Max(0, attempt - 1)).ConfigureAwait(false);
                settled = true;
                return;
            }

            TMessage? message;
            try
            {
                message = JsonSerializer.Deserialize<TMessage>(body, MessagingJson.Options);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await FailAsync(
                    channel,
                    delivery.DeliveryTag,
                    subscription,
                    envelope.Context,
                    body,
                    rawHeaders,
                    body,
                    attempt,
                    ex,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            if (message is null)
            {
                await FailAsync(
                    channel,
                    delivery.DeliveryTag,
                    subscription,
                    envelope.Context,
                    body,
                    rawHeaders,
                    body,
                    attempt,
                    new InvalidOperationException("The message body was empty."),
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            foreach (var registration in handlers)
            {
                if (registration.CancellationToken.IsCancellationRequested)
                {
                    await NackAsync(channel, delivery.DeliveryTag, requeue: true).ConfigureAwait(false);
                    settled = true;
                    return;
                }

                var typed = (Func<TMessage, MessageContext, CancellationToken, Task>)registration.Handler;
                MessageAttemptResult result;
                try
                {
                    result = await MessageDelivery.TryOnceAsync(
                        message,
                        envelope.Context,
                        typed,
                        _preHandlers,
                        registration.CancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (registration.CancellationToken.IsCancellationRequested)
                {
                    await NackAsync(channel, delivery.DeliveryTag, requeue: true).ConfigureAwait(false);
                    settled = true;
                    return;
                }

                if (result.Succeeded)
                    continue;

                await FailAsync(
                    channel,
                    delivery.DeliveryTag,
                    subscription,
                    envelope.Context,
                    message,
                    rawHeaders,
                    body,
                    attempt,
                    result.Error!,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, CancellationToken.None)
                .ConfigureAwait(false);
            settled = true;
            MessagingTelemetry.RecordConsume(subscription.DeclaredName, TimeProvider.System.GetElapsedTime(started));
        }
        catch (Exception ex)
        {
            consume?.SetStatus(ActivityStatusCode.Error, ex.Message);
            _logger.LogError(
                ex,
                "RabbitMQ consumer failed while handling {MessageType} on {Queue}",
                subscription.DeclaredName,
                subscription.Queue);
            if (!settled)
            {
                await DelayRedeliveryAsync(
                    channel,
                    delivery.DeliveryTag,
                    subscription,
                    body,
                    rawHeaders,
                    completedAttempts: attempt).ConfigureAwait(false);
            }
        }
        finally
        {
            if (acquired)
                subscription.Concurrency.Release();
        }
    }

    private async Task FailAsync(
        IChannel channel,
        ulong deliveryTag,
        Subscription subscription,
        MessageContext context,
        object message,
        IReadOnlyDictionary<string, object?>? headers,
        ReadOnlyMemory<byte> body,
        int attempt,
        Exception error,
        Activity? consume)
    {
        consume?.SetStatus(ActivityStatusCode.Error, error.Message);
        var willRetry = attempt < _options.MaxDeliveryAttempts;
        if (willRetry || !_options.DeadLetterFailedMessages)
        {
            if (willRetry)
            {
                MessagingTelemetry.RecordDeliveryRetry(
                    subscription.DeclaredName,
                    _options.SubscriptionName,
                    attempt,
                    context.MessageId,
                    _logger);
            }

            await DelayRedeliveryAsync(
                channel,
                deliveryTag,
                subscription,
                body,
                headers,
                completedAttempts: attempt).ConfigureAwait(false);
            return;
        }

        var errorType = error.GetType().Name;
        var errorText = $"{errorType}: {error.Message}";
        MessagingTelemetry.RecordDeadLettered(
            subscription.DeclaredName,
            _options.SubscriptionName,
            attempt,
            errorType,
            context.MessageId,
            errorText,
            _logger);
        _deadLetters.Enqueue(new DeadLetteredMessage(
            subscription.DeclaredName,
            message,
            context,
            attempt,
            errorText));
        await NackAsync(channel, deliveryTag, requeue: false).ConfigureAwait(false);
    }

    private async Task DelayRedeliveryAsync(
        IChannel consumerChannel,
        ulong deliveryTag,
        Subscription subscription,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, object?>? headers,
        int completedAttempts)
    {
        var table = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (headers is not null)
        {
            foreach (var pair in headers)
                table[pair.Key] = pair.Value;
        }

        table[RabbitMqHeaders.Attempt] = completedAttempts;
        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Headers = table,
        };

        var publishChannel = await PublishChannelAsync(CancellationToken.None).ConfigureAwait(false);
        await _publishLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            await publishChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: RabbitMqNames.RetryQueue(subscription.DeclaredName, _options.SubscriptionName),
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _publishLock.Release();
        }

        await consumerChannel.BasicAckAsync(deliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
    }

    private static bool IsUnroutable(PublishException exception)
        => exception.IsReturn || exception is PublishReturnException;

    private async Task NackAsync(IChannel channel, ulong deliveryTag, bool requeue)
    {
        try
        {
            if (channel.IsOpen)
            {
                await channel.BasicNackAsync(deliveryTag, multiple: false, requeue, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ nack failed for delivery {DeliveryTag}", deliveryTag);
        }
    }

    private async Task StopIfIdleAsync(Subscription subscription)
    {
        lock (subscription.Handlers)
        {
            if (subscription.Handlers.Count > 0)
                return;
        }

        var channel = subscription.Channel;
        var tag = subscription.ConsumerTag;
        if (channel is { IsOpen: true } && !string.IsNullOrEmpty(tag))
        {
            try
            {
                await channel.BasicCancelAsync(tag, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RabbitMQ consumer cancel failed for {Queue}", subscription.Queue);
            }
        }

        subscription.ConsumerTag = null;
        lock (subscription)
            subscription.Startup = null;
    }

    private async Task<IChannel> PublishChannelAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel is { IsOpen: true })
            return _publishChannel;

        await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_publishChannel is { IsOpen: true })
                return _publishChannel;

            _publishChannel = await _connector.CreateChannelAsync(
                new CreateChannelOptions(true, true, null, 1),
                cancellationToken).ConfigureAwait(false);
            return _publishChannel;
        }
        finally
        {
            _publishLock.Release();
        }
    }

    private static async Task WaitUntilCancelledAsync(
        Task starting,
        HandlerRegistration registration,
        CancellationTokenRegistration unregister)
    {
        try
        {
            await starting.ConfigureAwait(false);
            await registration.Completed.Task.ConfigureAwait(false);
        }
        finally
        {
            await unregister.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task CloseChannelAsync(IChannel? channel)
    {
        if (channel is null)
            return;

        try
        {
            await channel.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The broker may already have closed the channel.
        }
    }

    private sealed class Subscription
    {
        public Subscription(string declaredName, string queue, int concurrency)
        {
            DeclaredName = declaredName;
            Queue = queue;
            Concurrency = new SemaphoreSlim(concurrency, concurrency);
        }

        public string DeclaredName { get; }

        public string Queue { get; }

        public SemaphoreSlim Concurrency { get; }

        public List<HandlerRegistration> Handlers { get; } = [];

        public Task? Startup { get; set; }

        public IChannel? Channel { get; set; }

        public string? ConsumerTag { get; set; }
    }

    private sealed class HandlerRegistration
    {
        public HandlerRegistration(Delegate handler, CancellationToken cancellationToken)
        {
            Handler = handler;
            CancellationToken = cancellationToken;
        }

        public Delegate Handler { get; }

        public CancellationToken CancellationToken { get; }

        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
