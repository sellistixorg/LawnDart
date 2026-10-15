using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Telemetry;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Amazon SQS <see cref="IMessageTransport"/> with SNS for fan-out.
/// The topic is the name from <see cref="ITopicMapper"/>. The default mapper uses the declared message type name.
/// Each subscriber has one SQS queue subscribed to that topic with raw message delivery.
/// When several types share a topic, the subscription filter matches <c>lawndart_type</c>.
/// A failed handler is sent again to that same queue after <see cref="AwsSqsTransportOptions.RetryDelay"/>.
/// The retry is not published to the topic, so another subscription does not receive it.
/// A cancelled delivery uses the same wait and does not increment the attempt count.
/// When dead-letter handling is on, the last failure is sent to that subscription's dead-letter queue.
/// When it is off, the message is sent again and stays available.
/// Sending the copy and then deleting the original can leave both copies when the delete fails.
/// <see cref="Dispose"/> blocks for up to <see cref="AwsSqsTransportOptions.OperationTimeout"/>.
/// Prefer <see cref="DisposeAsync"/>.
/// User and tenant values are envelope headers. They are not an authorization context.
/// </summary>
public sealed class AwsSqsMessageTransport : IMessageTransport, IDisposable, IAsyncDisposable
{
    /// <summary>Value stored on <see cref="MessageContext.TransportType"/>. The value is <c>AwsSqs</c>.</summary>
    public const string TransportTypeName = "AwsSqs";

    private readonly ILogger<AwsSqsMessageTransport> _logger;
    private readonly AwsSqsTransportOptions _options;
    private readonly IReadOnlyList<IMessagePreHandler> _preHandlers;
    private readonly IDeadLetterQueue _deadLetters;
    private readonly AwsSqsConnector _connector;
    private readonly bool _ownsConnector;
    private readonly ITopicMapper _topicMapper;
    private readonly string[] _plannedTypes;
    private readonly ConcurrentDictionary<string, BrokerSubscription> _brokers = new(StringComparer.Ordinal);
    private int _disposed;

    /// <summary>
    /// Creates a transport that opens its own clients from <paramref name="options"/>.
    /// </summary>
    /// <param name="logger">Logger. A warning is written when a message is dead-lettered.</param>
    /// <param name="options">Region, endpoint, prefetch, retry, and dead-letter settings.</param>
    /// <param name="preHandlers">Hooks invoked before each handler attempt. May be empty.</param>
    /// <param name="deadLetters">In-process copy of messages sent to the dead-letter queue. A memory queue is used when omitted.</param>
    /// <param name="subscriptions">
    /// Message types the host recorded. The first subscribe writes a filter that lists every recorded type on that topic.
    /// A later subscribe does not remove a type from the filter.
    /// </param>
    public AwsSqsMessageTransport(
        ILogger<AwsSqsMessageTransport> logger,
        IOptions<AwsSqsTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers = null,
        IDeadLetterQueue? deadLetters = null,
        IEnumerable<MessageSubscription>? subscriptions = null)
        : this(logger, options, preHandlers, deadLetters, CreateOwnedConnector(options, logger), ownsConnector: true, subscriptions)
    {
    }

    internal AwsSqsMessageTransport(
        ILogger<AwsSqsMessageTransport> logger,
        IOptions<AwsSqsTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers,
        IDeadLetterQueue? deadLetters,
        AwsSqsConnector connector,
        bool ownsConnector,
        IEnumerable<MessageSubscription>? subscriptions = null)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connector);
        _logger = logger;
        _options = options.Value;
        _options.EnsureValid();
        _preHandlers = preHandlers?.ToArray() ?? [];
        _deadLetters = deadLetters ?? new MemoryDeadLetterQueue();
        _connector = connector;
        _ownsConnector = ownsConnector;
        _topicMapper = TopicMapperSelection.FromOptions(_options);
        _plannedTypes = Plan(subscriptions);
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
        var topic = _topicMapper.TopicFor(declaredName);
        AwsSqsNames.RequireTopic(topic);
        cancellationToken.ThrowIfCancellationRequested();

        using var publish = MessagingTelemetry.StartPublish(declaredName);
        publish?.SetTag("messaging.sns.topic", topic);
        var started = TimeProvider.System.GetTimestamp();
        var routed = true;

        try
        {
            await WaitForInFlightStartupAsync(topic, cancellationToken).ConfigureAwait(false);
            await _connector.EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);
            if (!_connector.IsRoutable(topic) &&
                !await _connector.HasQueueSubscriptionAsync(topic, cancellationToken).ConfigureAwait(false))
            {
                routed = false;
                if (_options.UnroutablePublish == AwsSqsUnroutablePublish.Throw)
                {
                    throw new InvalidOperationException(
                        $"No SQS queue is subscribed to '{topic}'. The message was not published.");
                }

                _logger.LogWarning(
                    "No SQS queue is subscribed to {Topic}. The message was not published. Message {MessageId}, type {MessageType}.",
                    topic,
                    context.MessageId,
                    declaredName);
            }
            else
            {
                _connector.MarkRoutable(topic);
                var body = JsonSerializer.Serialize(message, MessagingJson.Options);
                var attributes = AwsSqsHeaders.ForPublish(context, declaredName);
                var topicArn = await _connector.EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);
                await _connector.PublishAsync(topicArn, body, attributes, cancellationToken).ConfigureAwait(false);
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            MessagingTelemetry.RecordPublishError(declaredName);
            publish?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task SubscribeAsync<TMessage>(
        Func<TMessage, MessageContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        var topic = _topicMapper.TopicFor(declaredName);
        AwsSqsNames.RequireTopic(topic);
        AwsSqsNames.Queue(topic, _options.SubscriptionName);
        var broker = _brokers.GetOrAdd(topic, static (name, subscription) => new BrokerSubscription(name, subscription), _options.SubscriptionName);

        var registration = new HandlerRegistration(
            (payload, context, token) => MessageDelivery.TryOnceAsync(
                (TMessage)payload,
                context,
                handler,
                _preHandlers,
                token),
            cancellationToken);
        AddHandler(broker, typeof(TMessage), declaredName, registration);

        using var startupTimeout = new CancellationTokenSource(_options.OperationTimeout);
        try
        {
            await EnsureConsumerAsync(broker, startupTimeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (startupTimeout.IsCancellationRequested && ex is OperationCanceledException or TimeoutException)
        {
            RemoveHandler(broker, declaredName, registration);
            if (!HasHandlers(broker))
                await AbortStartupAsync(broker).ConfigureAwait(false);
            throw new TimeoutException(
                $"AWS SQS subscription for '{declaredName}' did not start within {_options.OperationTimeout}.",
                ex);
        }
        catch
        {
            RemoveHandler(broker, declaredName, registration);
            if (!HasHandlers(broker))
                await AbortStartupAsync(broker).ConfigureAwait(false);
            throw;
        }

        if (!cancellationToken.CanBeCanceled)
            return;

        var unregister = cancellationToken.Register(() =>
        {
            RemoveHandler(broker, declaredName, registration);
            _ = StopThenCompleteAsync(broker, registration);
        });

        try
        {
            await registration.Completed.Task.ConfigureAwait(false);
        }
        finally
        {
            await unregister.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Drops the handlers for <typeparamref name="TMessage"/> and leaves the consumer running.
    /// A delivery then waits <see cref="AwsSqsTransportOptions.RetryDelay"/> instead of calling a handler.
    /// </summary>
    /// <typeparam name="TMessage">The message type whose handlers to drop.</typeparam>
    internal void RemoveHandlers<TMessage>()
        where TMessage : class
    {
        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        var topic = _topicMapper.TopicFor(declaredName);
        if (!_brokers.TryGetValue(topic, out var broker))
            return;

        lock (broker.Gate)
        {
            if (!broker.Types.TryGetValue(declaredName, out var handlers))
                return;

            lock (handlers.Handlers)
                handlers.Handlers.Clear();
        }
    }

    /// <summary>
    /// Closes consumers and the clients this transport opened.
    /// Blocks the caller for up to <see cref="AwsSqsTransportOptions.OperationTimeout"/>.
    /// Prefer <see cref="DisposeAsync"/>. When the wait ends first, cleanup can still be running.
    /// </summary>
    public void Dispose()
    {
        var dispose = Task.Run(async () => await DisposeAsync().ConfigureAwait(false));
        if (!dispose.Wait(_options.OperationTimeout))
        {
            _logger.LogWarning(
                "Disposing the AWS SQS transport did not finish within {Timeout}. Prefer DisposeAsync.",
                _options.OperationTimeout);
        }
    }

    /// <summary>Closes consumers and the clients this transport opened.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var broker in _brokers.Values)
        {
            List<HandlerRegistration> handlers;
            lock (broker.Gate)
            {
                handlers = [];
                foreach (var typeHandlers in broker.Types.Values)
                {
                    lock (typeHandlers.Handlers)
                    {
                        handlers.AddRange(typeHandlers.Handlers);
                        typeHandlers.Handlers.Clear();
                    }
                }
            }

            await StopLoopAsync(broker).ConfigureAwait(false);
            foreach (var handler in handlers)
                handler.Completed.TrySetResult();
        }

        if (_ownsConnector)
            await _connector.DisposeAsync().ConfigureAwait(false);
    }

    private static string[] Plan(IEnumerable<MessageSubscription>? subscriptions)
    {
        if (subscriptions is null)
            return [];

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var subscription in subscriptions)
        {
            ArgumentNullException.ThrowIfNull(subscription);
            names.Add(MessageTypeNames.Require(subscription.MessageType));
        }

        return names.ToArray();
    }

    private static AwsSqsConnector CreateOwnedConnector(
        IOptions<AwsSqsTransportOptions> options,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        var value = options.Value;
        value.EnsureValid();
        Amazon.SQS.IAmazonSQS? sqs = null;
        try
        {
            sqs = AwsSqsClients.CreateSqs(value);
            var sns = AwsSqsClients.CreateSns(value);
            return new AwsSqsConnector(
                value,
                () => sqs,
                () => sns,
                ownsClients: true,
                logger);
        }
        catch
        {
            sqs?.Dispose();
            throw;
        }
    }

    private async Task EnsureConsumerAsync(BrokerSubscription broker, CancellationToken cancellationToken)
    {
        Task startup;
        lock (broker.Gate)
        {
            if (broker.Loop is null && (broker.Startup is null || broker.Startup.IsCompleted))
                broker.Startup = StartConsumerAsync(broker, cancellationToken);

            startup = broker.Startup ?? Task.CompletedTask;
        }

        try
        {
            await startup.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await ObserveStartupAsync(startup).ConfigureAwait(false);
            if (broker.Loop is null)
                await AbortStartupAsync(broker).ConfigureAwait(false);
            throw;
        }
        catch
        {
            if (broker.Loop is null)
                await AbortStartupAsync(broker).ConfigureAwait(false);
            throw;
        }

        await UpdateRouteAsync(broker, cancellationToken).ConfigureAwait(false);
    }

    private async Task ObserveStartupAsync(Task startup)
    {
        if (startup.IsCompleted)
            return;

        try
        {
            await startup.WaitAsync(_options.OperationTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "AWS SQS subscription startup ended after it was cancelled.");
        }
    }

    private async Task AbortStartupAsync(BrokerSubscription broker)
    {
        await StopLoopAsync(broker).ConfigureAwait(false);
        lock (broker.Gate)
            broker.Startup = null;
    }

    private async Task StartConsumerAsync(BrokerSubscription broker, CancellationToken cancellationToken)
    {
        var types = RuleTypes(broker);
        if (types.Length == 0)
            return;

        broker.QueueUrl = await _connector.EnsureSubscriptionAsync(
                broker.Topic,
                broker.Name,
                types,
                _options.ReplaceRouteRule,
                _options.DeadLetterFailedMessages,
                cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        broker.Stop = new CancellationTokenSource();
        broker.Slots = new SemaphoreSlim(_options.Concurrency, _options.Concurrency);
        broker.Loop = Task.Run(() => ReceiveLoopAsync(broker, broker.Stop.Token), CancellationToken.None);

        if (!HasHandlers(broker))
            await StopIfIdleAsync(broker).ConfigureAwait(false);
    }

    private async Task UpdateRouteAsync(BrokerSubscription broker, CancellationToken cancellationToken)
    {
        var types = RuleTypes(broker);
        if (types.Length == 0)
            return;

        broker.QueueUrl = await _connector.EnsureSubscriptionAsync(
                broker.Topic,
                broker.Name,
                types,
                _options.ReplaceRouteRule,
                _options.DeadLetterFailedMessages,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task WaitForInFlightStartupAsync(string topic, CancellationToken cancellationToken)
    {
        if (!_brokers.TryGetValue(topic, out var broker))
            return;

        Task? startup;
        lock (broker.Gate)
            startup = broker.Startup;

        if (startup is null || startup.IsCompleted)
            return;

        await startup.WaitAsync(_options.OperationTimeout, cancellationToken).ConfigureAwait(false);
    }

    private async Task ReceiveLoopAsync(BrokerSubscription broker, CancellationToken stopping)
    {
        var slots = broker.Slots ?? throw new InvalidOperationException("The receive loop has no concurrency slots.");
        while (!stopping.IsCancellationRequested)
        {
            var held = 0;
            try
            {
                await slots.WaitAsync(stopping).ConfigureAwait(false);
                held = 1;
                var batch = BatchSize();
                while (held < batch && slots.Wait(0))
                    held++;

                var messages = await _connector.ReceiveAsync(
                        broker.QueueUrl!,
                        held,
                        ReceiveWaitSeconds(),
                        VisibilitySeconds(),
                        stopping)
                    .ConfigureAwait(false);

                var extra = held - messages.Count;
                if (extra > 0)
                {
                    slots.Release(extra);
                    held -= extra;
                }

                foreach (var message in messages)
                {
                    held--;
                    var work = HandleThenReleaseAsync(broker, message, slots);
                    Track(broker, work);
                }
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
                ReleaseQuietly(slots, held);
                break;
            }
            catch (TimeoutException)
            {
                ReleaseQuietly(slots, held);
            }
            catch (Exception ex)
            {
                ReleaseQuietly(slots, held);
                _logger.LogError(ex, "AWS SQS receive failed for {Topic}/{Subscription}.", broker.Topic, broker.Name);
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200), stopping).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private async Task HandleThenReleaseAsync(BrokerSubscription broker, Message message, SemaphoreSlim slots)
    {
        try
        {
            await HandleDeliveryAsync(broker, message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "AWS SQS consumer failed while handling a message on {Topic}/{Subscription}.",
                broker.Topic,
                broker.Name);
        }
        finally
        {
            ReleaseQuietly(slots, 1);
        }
    }

    private async Task HandleDeliveryAsync(BrokerSubscription broker, Message message)
    {
        var body = message.Body ?? string.Empty;
        var attributes = message.MessageAttributes ?? new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        int completed;
        try
        {
            completed = AwsSqsHeaders.CompletedAttempts(attributes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AWS SQS could not read the attempt count on {Topic}.", broker.Topic);
            completed = 0;
        }

        var attempt = completed + 1;
        var declaredName = ResolveDeclaredName(broker, attributes);
        var messageType = declaredName ?? broker.Topic;
        using var consume = MessagingTelemetry.StartConsume(messageType, messageId: null);
        consume?.SetTag("messaging.sns.topic", broker.Topic);
        consume?.SetTag("messaging.sqs.queue", AwsSqsNames.Queue(broker.Topic, broker.Name));
        var receiveCount = ReadReceiveCount(message);
        consume?.SetTag("messaging.sqs.approximate_receive_count", receiveCount);
        consume?.SetTag("messaging.sqs.redelivered", completed > 0 || receiveCount > 1);
        var started = TimeProvider.System.GetTimestamp();
        var settled = false;

        if (declaredName is null)
        {
            await FailAsync(
                broker,
                messageType,
                message,
                attributes,
                body,
                new MessageContext { TransportType = TransportTypeName },
                attempt,
                new InvalidOperationException(
                    "The message did not include lawndart_type, and the subscription handles more than one event type."),
                consume).ConfigureAwait(false);
            return;
        }

        if (!IsAccepted(broker, declaredName) && !HasHandler(broker, declaredName))
        {
            _logger.LogDebug(
                "AWS SQS dropped {MessageType} on {Topic}/{Subscription}. This subscription does not handle it.",
                declaredName,
                broker.Topic,
                broker.Name);
            await DeleteQuietlyAsync(broker, message).ConfigureAwait(false);
            return;
        }

        TypeHandlers? typeHandlers;
        lock (broker.Gate)
            broker.Types.TryGetValue(declaredName, out typeHandlers);

        if (typeHandlers is null)
        {
            await DeferForHandlerAsync(broker, declaredName, message, attributes, body, completed, attempt, consume)
                .ConfigureAwait(false);
            return;
        }

        MessageEnvelope envelope;
        try
        {
            envelope = MessageEnvelopeHeaders.Read(AwsSqsHeaders.ToEnvelope(attributes), TransportTypeName);
            consume?.SetTag("messaging.message_id", envelope.Context.MessageId);
        }
        catch (Exception ex)
        {
            await FailAsync(
                broker,
                declaredName,
                message,
                attributes,
                body,
                new MessageContext { TransportType = TransportTypeName },
                attempt,
                ex,
                consume).ConfigureAwait(false);
            return;
        }

        try
        {
            List<HandlerRegistration> handlers;
            lock (typeHandlers.Handlers)
                handlers = [.. typeHandlers.Handlers];

            if (handlers.Count == 0)
            {
                await DeferForHandlerAsync(broker, declaredName, message, attributes, body, completed, attempt, consume)
                    .ConfigureAwait(false);
                return;
            }

            object? payload;
            try
            {
                payload = JsonSerializer.Deserialize(body, typeHandlers.MessageType, MessagingJson.Options);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await FailAsync(broker, declaredName, message, attributes, body, envelope.Context, attempt, ex, consume)
                    .ConfigureAwait(false);
                settled = true;
                return;
            }

            if (payload is null)
            {
                await FailAsync(
                    broker,
                    declaredName,
                    message,
                    attributes,
                    body,
                    envelope.Context,
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
                    await DelayRedeliveryAsync(broker, declaredName, message, attributes, body, completed)
                        .ConfigureAwait(false);
                    settled = true;
                    return;
                }

                MessageAttemptResult result;
                try
                {
                    result = await registration.Handle(payload, envelope.Context, registration.CancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (registration.CancellationToken.IsCancellationRequested)
                {
                    await DelayRedeliveryAsync(broker, declaredName, message, attributes, body, completed)
                        .ConfigureAwait(false);
                    settled = true;
                    return;
                }

                if (result.Succeeded)
                    continue;

                await FailAsync(
                    broker,
                    declaredName,
                    message,
                    attributes,
                    payload,
                    envelope.Context,
                    attempt,
                    result.Error!,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            await _connector.DeleteAsync(broker.QueueUrl!, message.ReceiptHandle, CancellationToken.None)
                .ConfigureAwait(false);
            settled = true;
            MessagingTelemetry.RecordConsume(declaredName, TimeProvider.System.GetElapsedTime(started));
        }
        catch (Exception ex)
        {
            consume?.SetStatus(ActivityStatusCode.Error, ex.Message);
            _logger.LogError(
                ex,
                "AWS SQS consumer failed while handling {MessageType} on {Topic}/{Subscription}.",
                declaredName,
                broker.Topic,
                broker.Name);
            if (!settled)
            {
                try
                {
                    await DelayRedeliveryAsync(broker, declaredName, message, attributes, body, attempt)
                        .ConfigureAwait(false);
                }
                catch (Exception settleEx)
                {
                    _logger.LogError(
                        settleEx,
                        "AWS SQS could not resend {MessageType} on {Topic}/{Subscription}.",
                        declaredName,
                        broker.Topic,
                        broker.Name);
                }
            }
        }
    }

    private async Task FailAsync(
        BrokerSubscription broker,
        string declaredName,
        Message message,
        IReadOnlyDictionary<string, MessageAttributeValue> attributes,
        object payload,
        MessageContext context,
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
                    declaredName,
                    _options.SubscriptionName,
                    attempt,
                    context.MessageId,
                    _logger);
            }

            await DelayRedeliveryAsync(
                broker,
                declaredName,
                message,
                attributes,
                payload as string ?? message.Body ?? string.Empty,
                completedAttempts: attempt).ConfigureAwait(false);
            return;
        }

        var errorType = error.GetType().Name;
        var errorText = $"{errorType}: {error.Message}";
        MessagingTelemetry.RecordDeadLettered(
            declaredName,
            _options.SubscriptionName,
            attempt,
            errorType,
            context.MessageId,
            errorText,
            _logger);
        _deadLetters.Enqueue(new DeadLetteredMessage(declaredName, payload, context, attempt, errorText));
        await DeadLetterAsync(broker, declaredName, message, attributes, message.Body ?? string.Empty, attempt)
            .ConfigureAwait(false);
    }

    private async Task DeferForHandlerAsync(
        BrokerSubscription broker,
        string declaredName,
        Message message,
        IReadOnlyDictionary<string, MessageAttributeValue> attributes,
        string body,
        int completed,
        int attempt,
        Activity? consume)
    {
        int deferrals;
        try
        {
            deferrals = AwsSqsHeaders.DeferralCount(attributes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AWS SQS could not read the deferral count on {Topic}.", broker.Topic);
            deferrals = 0;
        }

        var next = deferrals + 1;
        if (next <= _options.MaxHandlerDeferrals)
        {
            MessagingTelemetry.RecordDeferred(declaredName, _options.SubscriptionName, next);
            if (next >= AwsSqsTransportOptions.HandlerDeferralWarningThreshold)
            {
                _logger.LogWarning(
                    "No handler is registered for {MessageType} on {Topic}/{Subscription}. Deferral {Deferral} of {Cap}. The message returns after {Delay}.",
                    declaredName,
                    broker.Topic,
                    broker.Name,
                    next,
                    _options.MaxHandlerDeferrals,
                    _options.RetryDelay);
            }
            else
            {
                _logger.LogDebug(
                    "No handler is registered for {MessageType} on {Topic}/{Subscription}. Deferral {Deferral} of {Cap}. The message returns after {Delay}.",
                    declaredName,
                    broker.Topic,
                    broker.Name,
                    next,
                    _options.MaxHandlerDeferrals,
                    _options.RetryDelay);
            }

            await DelayRedeliveryAsync(broker, declaredName, message, attributes, body, completed, next)
                .ConfigureAwait(false);
            return;
        }

        await FailAsync(
            broker,
            declaredName,
            message,
            attributes,
            body,
            new MessageContext { TransportType = TransportTypeName },
            attempt,
            new InvalidOperationException($"No handler is registered for '{declaredName}'."),
            consume).ConfigureAwait(false);
    }

    private async Task DelayRedeliveryAsync(
        BrokerSubscription broker,
        string declaredName,
        Message message,
        IReadOnlyDictionary<string, MessageAttributeValue> attributes,
        string body,
        int completedAttempts,
        int? deferrals = null)
    {
        var resent = AwsSqsHeaders.ForRetry(attributes, declaredName, completedAttempts, deferrals);
        await _connector.SendAsync(
                broker.QueueUrl!,
                body,
                resent,
                AwsSqsNames.DelaySeconds(_options.RetryDelay),
                CancellationToken.None)
            .ConfigureAwait(false);
        await DeleteQuietlyAsync(broker, message).ConfigureAwait(false);
    }

    private async Task DeadLetterAsync(
        BrokerSubscription broker,
        string declaredName,
        Message message,
        IReadOnlyDictionary<string, MessageAttributeValue> attributes,
        string body,
        int completedAttempts)
    {
        var url = await _connector.EnsureDeadLetterUrlAsync(broker.Topic, broker.Name, CancellationToken.None)
            .ConfigureAwait(false);
        var forwarded = AwsSqsHeaders.ForRetry(attributes, declaredName, completedAttempts, deferrals: null);
        await _connector.SendAsync(url, body, forwarded, delaySeconds: 0, CancellationToken.None)
            .ConfigureAwait(false);
        await DeleteQuietlyAsync(broker, message).ConfigureAwait(false);
    }

    private async Task DeleteQuietlyAsync(BrokerSubscription broker, Message message)
    {
        try
        {
            await _connector.DeleteAsync(broker.QueueUrl!, message.ReceiptHandle, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "AWS SQS could not delete a message on {Topic}/{Subscription}. A second copy may be delivered.",
                broker.Topic,
                broker.Name);
        }
    }

    private string? ResolveDeclaredName(
        BrokerSubscription broker,
        IReadOnlyDictionary<string, MessageAttributeValue> attributes)
    {
        string? named;
        try
        {
            named = AwsSqsHeaders.EventTypeName(attributes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AWS SQS could not read lawndart_type on {Topic}.", broker.Topic);
            named = null;
        }

        lock (broker.Gate)
        {
            if (named is not null && (broker.Types.ContainsKey(named) || broker.AcceptedTypes.Contains(named)))
                return named;

            if (named is null && broker.AcceptedTypes.Count == 1)
            {
                foreach (var key in broker.AcceptedTypes)
                    return key;
            }

            if (named is null && broker.Types.Count == 1)
            {
                foreach (var key in broker.Types.Keys)
                    return key;
            }
        }

        return named;
    }

    private static int ReadReceiveCount(Message message)
    {
        if (message.Attributes is null)
            return 1;
        if (!message.Attributes.TryGetValue("ApproximateReceiveCount", out var text))
            return 1;
        return int.TryParse(text, out var count) ? count : 1;
    }

    private void AddHandler(
        BrokerSubscription broker,
        Type messageType,
        string declaredName,
        HandlerRegistration registration)
    {
        lock (broker.Gate)
        {
            if (!broker.Types.TryGetValue(declaredName, out var handlers))
            {
                handlers = new TypeHandlers(messageType, declaredName);
                broker.Types[declaredName] = handlers;
            }

            lock (handlers.Handlers)
                handlers.Handlers.Add(registration);
        }
    }

    private static void RemoveHandler(
        BrokerSubscription broker,
        string declaredName,
        HandlerRegistration registration)
    {
        lock (broker.Gate)
        {
            if (!broker.Types.TryGetValue(declaredName, out var handlers))
                return;

            lock (handlers.Handlers)
                handlers.Handlers.Remove(registration);
        }
    }

    private static bool HasHandlers(BrokerSubscription broker)
    {
        lock (broker.Gate)
        {
            foreach (var handlers in broker.Types.Values)
            {
                lock (handlers.Handlers)
                {
                    if (handlers.Handlers.Count > 0)
                        return true;
                }
            }
        }

        return false;
    }

    private static bool HasHandler(BrokerSubscription broker, string declaredName)
    {
        lock (broker.Gate)
            return broker.Types.ContainsKey(declaredName);
    }

    private string[] RuleTypes(BrokerSubscription broker)
    {
        var accepted = Acceptance(broker);
        if (!_options.ReplaceRouteRule)
            return accepted;

        var recorded = new List<string>();
        foreach (var declared in _plannedTypes)
        {
            if (string.Equals(_topicMapper.TopicFor(declared), broker.Topic, StringComparison.Ordinal))
                recorded.Add(declared);
        }

        recorded.Sort(StringComparer.Ordinal);
        return recorded.Count > 0 ? recorded.ToArray() : accepted;
    }

    private string[] Acceptance(BrokerSubscription broker)
    {
        lock (broker.Gate)
        {
            foreach (var declared in _plannedTypes)
            {
                if (string.Equals(_topicMapper.TopicFor(declared), broker.Topic, StringComparison.Ordinal))
                    broker.AcceptedTypes.Add(declared);
            }

            foreach (var name in broker.Types.Keys)
                broker.AcceptedTypes.Add(name);

            return broker.AcceptedTypes.OrderBy(static name => name, StringComparer.Ordinal).ToArray();
        }
    }

    private static bool IsAccepted(BrokerSubscription broker, string declaredName)
    {
        lock (broker.Gate)
            return broker.AcceptedTypes.Contains(declaredName) || broker.Types.ContainsKey(declaredName);
    }

    private int BatchSize()
    {
        var prefetch = _options.Prefetch <= 0 ? 1 : _options.Prefetch;
        return Math.Min(prefetch, AwsSqsTransportOptions.MaxMessagesPerReceive);
    }

    private int ReceiveWaitSeconds()
    {
        var wait = (int)Math.Ceiling(_options.LongPollWait.TotalSeconds);
        if (wait < 0)
            wait = 0;
        if (wait > AwsSqsTransportOptions.MaxLongPollSeconds)
            wait = AwsSqsTransportOptions.MaxLongPollSeconds;
        var budget = (int)_options.OperationTimeout.TotalSeconds;
        if (budget >= 0 && wait > budget)
            wait = budget;
        return wait;
    }

    private int VisibilitySeconds()
    {
        var seconds = (int)Math.Ceiling(_options.VisibilityTimeout.TotalSeconds);
        if (seconds < 1)
            return 1;
        if (seconds > AwsSqsTransportOptions.MaxVisibilitySeconds)
            return AwsSqsTransportOptions.MaxVisibilitySeconds;
        return seconds;
    }

    private async Task StopThenCompleteAsync(BrokerSubscription broker, HandlerRegistration registration)
    {
        try
        {
            await StopIfIdleAsync(broker).ConfigureAwait(false);
        }
        finally
        {
            registration.Completed.TrySetResult();
        }
    }

    private async Task StopIfIdleAsync(BrokerSubscription broker)
    {
        if (HasHandlers(broker))
            return;

        await StopLoopAsync(broker).ConfigureAwait(false);
        lock (broker.Gate)
            broker.Startup = null;
    }

    private async Task StopLoopAsync(BrokerSubscription broker)
    {
        var stop = broker.Stop;
        var loop = broker.Loop;
        if (stop is null && loop is null)
            return;

        broker.Stop = null;
        broker.Loop = null;
        try
        {
            stop?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        if (loop is not null)
        {
            try
            {
                await loop.WaitAsync(_options.OperationTimeout).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "AWS SQS receive loop stop timed out for {Topic}/{Subscription}.", broker.Topic, broker.Name);
            }
        }

        Task[] pending;
        lock (broker.Gate)
            pending = broker.InFlight.ToArray();
        if (pending.Length > 0)
        {
            try
            {
                await Task.WhenAll(pending).WaitAsync(_options.OperationTimeout).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "AWS SQS in-flight handlers did not finish for {Topic}/{Subscription}.", broker.Topic, broker.Name);
            }
        }

        broker.Slots?.Dispose();
        broker.Slots = null;
        stop?.Dispose();
    }

    private static void Track(BrokerSubscription broker, Task work)
    {
        lock (broker.Gate)
            broker.InFlight.Add(work);
        _ = work.ContinueWith(
            static (task, state) =>
            {
                var target = (BrokerSubscription)state!;
                lock (target.Gate)
                    target.InFlight.Remove(task);
            },
            broker,
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
    }

    private static void ReleaseQuietly(SemaphoreSlim slots, int count)
    {
        if (count <= 0)
            return;

        try
        {
            slots.Release(count);
        }
        catch (ObjectDisposedException)
        {
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private sealed class BrokerSubscription
    {
        public BrokerSubscription(string topic, string name)
        {
            Topic = topic;
            Name = name;
        }

        public string Topic { get; }

        public string Name { get; }

        public string? QueueUrl { get; set; }

        public Dictionary<string, TypeHandlers> Types { get; } = new(StringComparer.Ordinal);

        public HashSet<string> AcceptedTypes { get; } = new(StringComparer.Ordinal);

        public List<Task> InFlight { get; } = [];

        public object Gate { get; } = new();

        public Task? Startup { get; set; }

        public Task? Loop { get; set; }

        public CancellationTokenSource? Stop { get; set; }

        public SemaphoreSlim? Slots { get; set; }
    }

    private sealed class TypeHandlers
    {
        public TypeHandlers(Type messageType, string declaredName)
        {
            MessageType = messageType;
            DeclaredName = declaredName;
        }

        public Type MessageType { get; }

        public string DeclaredName { get; }

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
