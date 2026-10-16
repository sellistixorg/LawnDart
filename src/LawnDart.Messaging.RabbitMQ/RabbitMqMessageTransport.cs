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
/// The exchange is the topic from <see cref="ITopicMapper"/>. The default mapper uses the declared message type name.
/// The routing key is the declared message type name, so a queue bound for one type does not receive another.
/// Publish declares that exchange. A subscription declares its queue and binds it with the routing key.
/// <see cref="RabbitMqQueueLayout.Subscription"/> uses one queue for every type that subscription handles.
/// <see cref="RabbitMqQueueLayout.EventType"/> uses one queue per event type.
/// Retry and dead-letter resources are one set per subscription in both layouts, routed by event type name.
/// A failed handler waits on the retry queue, then returns, until <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
/// A cancelled delivery uses that same wait and does not increment the attempt count.
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
    private readonly ITopicMapper _topicMapper;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private readonly ConcurrentDictionary<string, QueueConsumer> _consumers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string> _typeQueues = new();
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
        _topicMapper = TopicMapperSelection.FromOptions(_options);
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
        var exchange = _topicMapper.TopicFor(declaredName);
        var routingKey = RabbitMqNames.RoutingKey(declaredName);
        RabbitMqNames.RequireExchange(exchange);
        RabbitMqNames.RequireRoutingKey(routingKey);
        cancellationToken.ThrowIfCancellationRequested();

        using var publish = MessagingTelemetry.StartPublish(declaredName);
        publish?.SetTag("messaging.rabbitmq.exchange", exchange);
        publish?.SetTag("messaging.rabbitmq.routing_key", routingKey);
        var started = TimeProvider.System.GetTimestamp();

        try
        {
            await _connector.DeclareExchangeAsync(exchange, cancellationToken).ConfigureAwait(false);

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
            var routed = true;

            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                try
                {
                    await channel.BasicPublishAsync(
                        exchange,
                        routingKey,
                        mandatory: true,
                        properties,
                        body,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (PublishException ex) when (IsUnroutable(ex) &&
                    _options.UnroutablePublish == RabbitMqUnroutablePublish.Warn)
                {
                    routed = false;
                    _logger.LogWarning(
                        ex,
                        "No queue is bound for {MessageType}. The message was not queued. Exchange {Exchange}, routing key {RoutingKey}, message {MessageId}.",
                        declaredName,
                        exchange,
                        routingKey,
                        context.MessageId);
                }
            }
            finally
            {
                _publishLock.Release();
            }

            if (routed)
            {
                MessagingTelemetry.RecordPublish(declaredName, TimeProvider.System.GetElapsedTime(started));
            }
            else
            {
                publish?.SetTag("messaging.publish.outcome", "unroutable");
                publish?.SetStatus(ActivityStatusCode.Error, "unroutable");
                MessagingTelemetry.RecordPublishUnroutable(declaredName);
            }
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
        var exchange = _topicMapper.TopicFor(declaredName);
        var queue = RabbitMqNames.Queue(_options.SubscriptionName, _options.QueueLayout, declaredName);
        RabbitMqNames.RequireExchange(exchange);
        RabbitMqNames.RequireConsumerNames(_options.SubscriptionName, _options.QueueLayout, declaredName);
        var consumer = _consumers.GetOrAdd(
            queue,
            name => new QueueConsumer(name, _options.Concurrency));
        _typeQueues[typeof(TMessage)] = queue;

        var registration = new HandlerRegistration(
            (message, context, token) => MessageDelivery.TryOnceAsync(
                (TMessage)message,
                context,
                handler,
                _preHandlers,
                token),
            cancellationToken);
        AddHandler(consumer, typeof(TMessage), declaredName, registration);

        // The queue is bound before this method returns a task, so the next publish can route.
        EnsureConsumerAsync(consumer, declaredName, exchange).GetAwaiter().GetResult();

        if (!cancellationToken.CanBeCanceled)
            return Task.CompletedTask;

        var unregister = cancellationToken.Register(() =>
        {
            RemoveRegistration(consumer, declaredName, registration);
            registration.Completed.TrySetResult();
            _ = StopIfIdleAsync(consumer);
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
        if (!_typeQueues.TryGetValue(typeof(TMessage), out var queue) ||
            !_consumers.TryGetValue(queue, out var consumer))
        {
            return;
        }

        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        lock (consumer.Gate)
        {
            if (!consumer.Types.TryGetValue(declaredName, out var handlers))
                return;

            lock (handlers.Handlers)
                handlers.Handlers.Clear();
        }
    }

    /// <summary>Closes consumers and the connection this transport opened.</summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>Closes consumers and the connection this transport opened.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var consumer in _consumers.Values)
        {
            List<HandlerRegistration> handlers = [];
            lock (consumer.Gate)
            {
                foreach (var typeHandlers in consumer.Types.Values)
                {
                    lock (typeHandlers.Handlers)
                    {
                        handlers.AddRange(typeHandlers.Handlers);
                        typeHandlers.Handlers.Clear();
                    }
                }
            }

            foreach (var handler in handlers)
                handler.Completed.TrySetResult();

            await CloseChannelAsync(consumer.Channel).ConfigureAwait(false);
            consumer.Channel = null;
            consumer.Concurrency.Dispose();
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

    private async Task EnsureConsumerAsync(QueueConsumer consumer, string declaredName, string exchange)
    {
        await _connector.DeclareSubscriptionAsync(
                declaredName,
                exchange,
                _options.SubscriptionName,
                CancellationToken.None)
            .ConfigureAwait(false);

        Task startup;
        lock (consumer.Gate)
        {
            consumer.Startup ??= StartConsumerAsync(consumer);
            startup = consumer.Startup;
        }

        try
        {
            await startup.ConfigureAwait(false);
        }
        catch
        {
            lock (consumer.Gate)
            {
                if (ReferenceEquals(consumer.Startup, startup))
                    consumer.Startup = null;
            }

            throw;
        }
    }

    private async Task StartConsumerAsync(QueueConsumer consumer)
    {
        var channel = consumer.Channel;
        if (channel is not { IsOpen: true })
        {
            var concurrency = checked((ushort)_options.Concurrency);
            channel = await _connector.CreateChannelAsync(
                new CreateChannelOptions(false, false, null, concurrency),
                CancellationToken.None).ConfigureAwait(false);
            consumer.Channel = channel;

            var prefetch = checked((ushort)_options.Prefetch);
            await channel.BasicQosAsync(0, prefetch, global: false, CancellationToken.None).ConfigureAwait(false);
        }

        var basicConsumer = new AsyncEventingBasicConsumer(channel);
        basicConsumer.ReceivedAsync += (_, delivery) =>
            HandleDeliveryAsync(consumer, channel, delivery);

        var tag = await channel.BasicConsumeAsync(
            consumer.Queue,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: basicConsumer,
            cancellationToken: CancellationToken.None).ConfigureAwait(false);

        consumer.ConsumerTag = tag;

        if (!HasHandlers(consumer))
            await StopIfIdleAsync(consumer).ConfigureAwait(false);
    }

    private async Task HandleDeliveryAsync(
        QueueConsumer consumer,
        IChannel channel,
        BasicDeliverEventArgs delivery)
    {
        var body = delivery.Body.ToArray();
        var rawHeaders = RabbitMqHeaders.Copy(delivery.BasicProperties.Headers);
        var completed = RabbitMqHeaders.CompletedAttempts(rawHeaders);
        var attempt = completed + 1;
        var envelope = MessageEnvelopeHeaders.Read(
            RabbitMqHeaders.ToEnvelope(rawHeaders),
            TransportTypeName);
        var declaredName = ResolveDeclaredName(consumer, delivery, envelope);
        var messageType = declaredName
            ?? (string.IsNullOrEmpty(delivery.RoutingKey) ? consumer.Queue : delivery.RoutingKey);
        var routingKey = declaredName
            ?? (string.IsNullOrEmpty(delivery.RoutingKey) ? messageType : delivery.RoutingKey);

        using var consume = MessagingTelemetry.StartConsume(messageType, envelope.Context.MessageId);
        consume?.SetTag("messaging.rabbitmq.queue", consumer.Queue);
        consume?.SetTag("messaging.rabbitmq.redelivered", delivery.Redelivered || completed > 0);
        var started = TimeProvider.System.GetTimestamp();
        var settled = false;
        var acquired = false;

        try
        {
            await consumer.Concurrency.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            acquired = true;

            TypeHandlers? typeHandlers = null;
            if (declaredName is not null)
            {
                lock (consumer.Gate)
                    consumer.Types.TryGetValue(declaredName, out typeHandlers);
            }

            var handlers = typeHandlers is null ? [] : CopyHandlers(typeHandlers);
            if (handlers.Count == 0)
            {
                _logger.LogDebug(
                    "No handler is registered for {MessageType} on {Queue}. The message returns after {Delay}.",
                    messageType,
                    consumer.Queue,
                    _options.RetryDelay);
                await DelayRedeliveryAsync(
                    channel,
                    delivery.DeliveryTag,
                    routingKey,
                    body,
                    rawHeaders,
                    completedAttempts: Math.Max(0, attempt - 1)).ConfigureAwait(false);
                settled = true;
                return;
            }

            object? message;
            try
            {
                message = JsonSerializer.Deserialize(body, typeHandlers!.MessageType, MessagingJson.Options);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await FailAsync(
                    channel,
                    delivery.DeliveryTag,
                    messageType,
                    routingKey,
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
                    messageType,
                    routingKey,
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
                    await DelayRedeliveryAsync(
                        channel,
                        delivery.DeliveryTag,
                        routingKey,
                        body,
                        rawHeaders,
                        completedAttempts: completed).ConfigureAwait(false);
                    settled = true;
                    return;
                }

                MessageAttemptResult result;
                try
                {
                    result = await registration.Handle(message, envelope.Context, registration.CancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (registration.CancellationToken.IsCancellationRequested)
                {
                    await DelayRedeliveryAsync(
                        channel,
                        delivery.DeliveryTag,
                        routingKey,
                        body,
                        rawHeaders,
                        completedAttempts: completed).ConfigureAwait(false);
                    settled = true;
                    return;
                }

                if (result.Succeeded)
                    continue;

                await FailAsync(
                    channel,
                    delivery.DeliveryTag,
                    messageType,
                    routingKey,
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
            MessagingTelemetry.RecordConsume(messageType, TimeProvider.System.GetElapsedTime(started));
        }
        catch (Exception ex)
        {
            consume?.SetStatus(ActivityStatusCode.Error, ex.Message);
            _logger.LogError(
                ex,
                "RabbitMQ consumer failed while handling {MessageType} on {Queue}",
                messageType,
                consumer.Queue);
            if (!settled)
            {
                await DelayRedeliveryAsync(
                    channel,
                    delivery.DeliveryTag,
                    routingKey,
                    body,
                    rawHeaders,
                    completedAttempts: attempt).ConfigureAwait(false);
            }
        }
        finally
        {
            if (acquired)
            {
                try
                {
                    consumer.Concurrency.Release();
                }
                catch (ObjectDisposedException)
                {
                    // The transport is already disposed.
                }
            }
        }
    }

    private async Task FailAsync(
        IChannel channel,
        ulong deliveryTag,
        string messageType,
        string routingKey,
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
                    messageType,
                    _options.SubscriptionName,
                    attempt,
                    context.MessageId,
                    _logger);
            }

            await DelayRedeliveryAsync(
                channel,
                deliveryTag,
                routingKey,
                body,
                headers,
                completedAttempts: attempt).ConfigureAwait(false);
            return;
        }

        var errorType = error.GetType().Name;
        var errorText = $"{errorType}: {error.Message}";
        MessagingTelemetry.RecordDeadLettered(
            messageType,
            _options.SubscriptionName,
            attempt,
            errorType,
            context.MessageId,
            errorText,
            _logger);
        _deadLetters.Enqueue(new DeadLetteredMessage(
            messageType,
            message,
            context,
            attempt,
            errorText));
        await NackAsync(channel, deliveryTag, requeue: false).ConfigureAwait(false);
    }

    private async Task DelayRedeliveryAsync(
        IChannel consumerChannel,
        ulong deliveryTag,
        string routingKey,
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
        if (!table.ContainsKey(MessageHeaders.MessageType))
            table[MessageHeaders.MessageType] = routingKey;

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Type = routingKey,
            Headers = table,
        };

        var publishChannel = await PublishChannelAsync(CancellationToken.None).ConfigureAwait(false);
        await _publishLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            await publishChannel.BasicPublishAsync(
                exchange: RabbitMqNames.RetryExchange(_options.SubscriptionName),
                routingKey: routingKey,
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

    private async Task StopIfIdleAsync(QueueConsumer consumer)
    {
        IChannel? channel;
        string? tag;
        lock (consumer.Gate)
        {
            if (HasHandlersNoLock(consumer))
                return;

            tag = consumer.ConsumerTag;
            channel = consumer.Channel;
            consumer.ConsumerTag = null;
            consumer.Startup = null;
        }

        if (channel is { IsOpen: true } && !string.IsNullOrEmpty(tag))
        {
            try
            {
                await channel.BasicCancelAsync(tag, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "RabbitMQ consumer cancel failed for {Queue}", consumer.Queue);
            }
        }
    }

    private static string? ResolveDeclaredName(
        QueueConsumer consumer,
        BasicDeliverEventArgs delivery,
        MessageEnvelope envelope)
    {
        lock (consumer.Gate)
        {
            if (!string.IsNullOrEmpty(delivery.RoutingKey) && consumer.Types.ContainsKey(delivery.RoutingKey))
                return delivery.RoutingKey;

            if (envelope.MessageType is not null && consumer.Types.ContainsKey(envelope.MessageType))
                return envelope.MessageType;

            var typeName = delivery.BasicProperties.Type;
            if (!string.IsNullOrEmpty(typeName) && consumer.Types.ContainsKey(typeName))
                return typeName;

            if (consumer.Types.Count == 1)
            {
                foreach (var key in consumer.Types.Keys)
                    return key;
            }
        }

        return null;
    }

    private static void AddHandler(
        QueueConsumer consumer,
        Type messageType,
        string declaredName,
        HandlerRegistration registration)
    {
        lock (consumer.Gate)
        {
            if (!consumer.Types.TryGetValue(declaredName, out var handlers))
            {
                handlers = new TypeHandlers(messageType);
                consumer.Types[declaredName] = handlers;
            }

            lock (handlers.Handlers)
                handlers.Handlers.Add(registration);
        }
    }

    private static void RemoveRegistration(
        QueueConsumer consumer,
        string declaredName,
        HandlerRegistration registration)
    {
        lock (consumer.Gate)
        {
            if (!consumer.Types.TryGetValue(declaredName, out var handlers))
                return;

            lock (handlers.Handlers)
                handlers.Handlers.Remove(registration);
        }
    }

    private static List<HandlerRegistration> CopyHandlers(TypeHandlers typeHandlers)
    {
        lock (typeHandlers.Handlers)
            return [.. typeHandlers.Handlers];
    }

    private static bool HasHandlers(QueueConsumer consumer)
    {
        lock (consumer.Gate)
            return HasHandlersNoLock(consumer);
    }

    private static bool HasHandlersNoLock(QueueConsumer consumer)
    {
        foreach (var typeHandlers in consumer.Types.Values)
        {
            lock (typeHandlers.Handlers)
            {
                if (typeHandlers.Handlers.Count > 0)
                    return true;
            }
        }

        return false;
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

    private sealed class QueueConsumer
    {
        public QueueConsumer(string queue, int concurrency)
        {
            Queue = queue;
            Concurrency = new SemaphoreSlim(concurrency, concurrency);
        }

        public string Queue { get; }

        public Dictionary<string, TypeHandlers> Types { get; } = new(StringComparer.Ordinal);

        public object Gate { get; } = new();

        public SemaphoreSlim Concurrency { get; }

        public Task? Startup { get; set; }

        public IChannel? Channel { get; set; }

        public string? ConsumerTag { get; set; }
    }

    private sealed class TypeHandlers
    {
        public TypeHandlers(Type messageType) => MessageType = messageType;

        public Type MessageType { get; }

        public List<HandlerRegistration> Handlers { get; } = [];
    }

    private sealed class HandlerRegistration
    {
        public HandlerRegistration(
            Func<object, MessageContext, CancellationToken, Task<MessageAttemptResult>> handle,
            CancellationToken cancellationToken)
        {
            Handle = handle;
            CancellationToken = cancellationToken;
        }

        public Func<object, MessageContext, CancellationToken, Task<MessageAttemptResult>> Handle { get; }

        public CancellationToken CancellationToken { get; }

        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
