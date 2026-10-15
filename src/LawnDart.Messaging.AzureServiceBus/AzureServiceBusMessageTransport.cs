using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Telemetry;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Azure Service Bus <see cref="IMessageTransport"/>.
/// The topic is the name from <see cref="ITopicMapper"/>. The default mapper uses the declared message type name.
/// Each subscriber has one subscription on that topic.
/// When several types share a topic, the subscription rule matches <c>lawndart_type</c>
/// and <c>lawndart_target</c>. One processor on that subscription dispatches each type to its handlers.
/// Publish creates the topic. A subscription creates its own subscription entity.
/// A failed handler is scheduled again after <see cref="AzureServiceBusTransportOptions.RetryDelay"/>
/// until <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
/// When dead-letter handling is on, the last failure is dead-lettered on the subscription.
/// When it is off, the message is scheduled again and stays available.
/// A cancelled delivery uses the same wait. It is not abandoned immediately.
/// A retry sets <c>lawndart_target</c> to that subscription.
/// A publish sets it to <c>*</c>. A message with no <c>lawndart_target</c> reaches every subscription
/// that accepts the event type. A message with no <c>lawndart_type</c> matches the type clause.
/// Scheduling the retry and then completing the original can leave both copies
/// when the complete fails after the schedule succeeds.
/// <see cref="Dispose"/> blocks for up to <see cref="AzureServiceBusTransportOptions.OperationTimeout"/>.
/// Prefer <see cref="DisposeAsync"/>.
/// </summary>
public sealed class AzureServiceBusMessageTransport : IMessageTransport, IDisposable, IAsyncDisposable
{
    /// <summary>Value stored on <see cref="MessageContext.TransportType"/>. The value is <c>AzureServiceBus</c>.</summary>
    public const string TransportTypeName = "AzureServiceBus";

    private readonly ILogger<AzureServiceBusMessageTransport> _logger;
    private readonly AzureServiceBusTransportOptions _options;
    private readonly IReadOnlyList<IMessagePreHandler> _preHandlers;
    private readonly IDeadLetterQueue _deadLetters;
    private readonly AzureServiceBusConnector _connector;
    private readonly bool _ownsConnector;
    private readonly ITopicMapper _topicMapper;
    private readonly string[] _plannedTypes;
    private readonly ConcurrentDictionary<string, BrokerSubscription> _brokers = new(StringComparer.Ordinal);
    private int _disposed;

    /// <summary>
    /// Creates a transport that opens its own clients from <paramref name="options"/>.
    /// </summary>
    /// <param name="logger">Logger. A warning is written when a message is dead-lettered.</param>
    /// <param name="options">Connection, prefetch, retry, and dead-letter settings.</param>
    /// <param name="preHandlers">Hooks invoked before each handler attempt. May be empty.</param>
    /// <param name="deadLetters">In-process copy of messages dead-lettered on the subscription. A memory queue is used when omitted.</param>
    /// <param name="subscriptions">
    /// Message types the host recorded. The first subscribe writes a rule that lists every recorded type on that topic.
    /// A later subscribe does not remove a type from the rule.
    /// </param>
    public AzureServiceBusMessageTransport(
        ILogger<AzureServiceBusMessageTransport> logger,
        IOptions<AzureServiceBusTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers = null,
        IDeadLetterQueue? deadLetters = null,
        IEnumerable<MessageSubscription>? subscriptions = null)
        : this(logger, options, preHandlers, deadLetters, CreateOwnedConnector(options, logger), ownsConnector: true, subscriptions)
    {
    }

    internal AzureServiceBusMessageTransport(
        ILogger<AzureServiceBusMessageTransport> logger,
        IOptions<AzureServiceBusTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers,
        IDeadLetterQueue? deadLetters,
        AzureServiceBusConnector connector,
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

    private static AzureServiceBusConnector CreateOwnedConnector(
        IOptions<AzureServiceBusTransportOptions> options,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        var value = options.Value;
        value.EnsureValid();
        if (!value.HasDataConnection || !value.HasAdminConnection)
        {
            throw new InvalidOperationException(
                "Set a connection string, or a fully qualified namespace and credential, or pass a ServiceBusClient and a ServiceBusAdministrationClient.");
        }

        var client = AzureServiceBusClients.CreateClient(value);
        var admin = AzureServiceBusClients.CreateAdmin(value);
        return new AzureServiceBusConnector(
            value,
            () => client,
            () => admin,
            ownsClient: true,
            ownsAdmin: true,
            logger);
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
        AzureServiceBusNames.RequireTopic(topic);
        cancellationToken.ThrowIfCancellationRequested();

        using var publish = MessagingTelemetry.StartPublish(declaredName);
        publish?.SetTag("messaging.servicebus.topic", topic);
        var started = TimeProvider.System.GetTimestamp();

        try
        {
            await WaitForInFlightStartupAsync(topic, cancellationToken).ConfigureAwait(false);
            await _connector.EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);
            var body = JsonSerializer.SerializeToUtf8Bytes(message, MessagingJson.Options);
            var brokerMessage = CreateOutbound(body, context, declaredName);
            var sender = await _connector.SenderAsync(topic, cancellationToken).ConfigureAwait(false);
            using var timeout = LinkTimeout(cancellationToken);
            await sender.SendMessageAsync(brokerMessage, timeout.Token).ConfigureAwait(false);
            MessagingTelemetry.RecordPublish(declaredName, TimeProvider.System.GetElapsedTime(started));
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
        AzureServiceBusNames.RequireTopic(topic);
        var broker = _brokers.GetOrAdd(
            topic,
            static (name, subscriptionName) => new BrokerSubscription(name, subscriptionName),
            _options.SubscriptionName);

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
                $"Azure Service Bus subscription for '{declaredName}' did not start within {_options.OperationTimeout}.",
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
    /// Drops the handlers for <typeparamref name="TMessage"/> and leaves the processor running.
    /// A delivery then waits <see cref="AzureServiceBusTransportOptions.RetryDelay"/> instead of calling a handler.
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
    /// Closes processors and the clients this transport opened.
    /// Blocks the caller for up to <see cref="AzureServiceBusTransportOptions.OperationTimeout"/>.
    /// Prefer <see cref="DisposeAsync"/>. When the wait ends first, cleanup can still be running.
    /// </summary>
    public void Dispose()
    {
        var dispose = Task.Run(async () => await DisposeAsync().ConfigureAwait(false));
        if (!dispose.Wait(_options.OperationTimeout))
        {
            _logger.LogWarning(
                "Disposing the Azure Service Bus transport did not finish within {Timeout}. Prefer DisposeAsync.",
                _options.OperationTimeout);
        }
    }

    /// <summary>Closes processors and the clients this transport opened.</summary>
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

            await StopProcessorAsync(broker).ConfigureAwait(false);
            foreach (var handler in handlers)
                handler.Completed.TrySetResult();
        }

        if (_ownsConnector)
            await _connector.DisposeAsync().ConfigureAwait(false);
    }

    private async Task EnsureConsumerAsync(BrokerSubscription broker, CancellationToken cancellationToken)
    {
        Task startup;
        lock (broker.Gate)
        {
            if (broker.Processor is null && (broker.Startup is null || broker.Startup.IsCompleted))
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
            if (broker.Processor is null)
                await AbortStartupAsync(broker).ConfigureAwait(false);
            throw;
        }
        catch
        {
            if (broker.Processor is null)
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
            _logger.LogDebug(ex, "Azure Service Bus subscription startup ended after it was cancelled.");
        }
    }

    private async Task AbortStartupAsync(BrokerSubscription broker)
    {
        await StopProcessorAsync(broker).ConfigureAwait(false);
        lock (broker.Gate)
            broker.Startup = null;
    }

    private async Task StartConsumerAsync(BrokerSubscription broker, CancellationToken cancellationToken)
    {
        var types = Acceptance(broker);
        if (types.Length == 0)
            return;

        await _connector.EnsureSubscriptionAsync(broker.Topic, broker.Name, types, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        var processor = _connector.Client.CreateProcessor(
            broker.Topic,
            broker.Name,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = _options.Concurrency,
                PrefetchCount = _options.Prefetch,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                Identifier = broker.Name,
            });

        processor.ProcessMessageAsync += args => HandleDeliveryAsync(broker, args);
        processor.ProcessErrorAsync += args =>
        {
            _logger.LogError(
                args.Exception,
                "Azure Service Bus processor failed for {Topic}/{Subscription}. Source {ErrorSource}.",
                broker.Topic,
                broker.Name,
                args.ErrorSource);
            return Task.CompletedTask;
        };

        try
        {
            await processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            broker.Processor = processor;
        }
        catch
        {
            try
            {
                await processor.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception disposeEx)
            {
                _logger.LogDebug(disposeEx, "Azure Service Bus processor dispose failed for {Topic}/{Subscription}.", broker.Topic, broker.Name);
            }

            throw;
        }

        if (!HasHandlers(broker))
            await StopIfIdleAsync(broker).ConfigureAwait(false);
    }

    private async Task UpdateRouteAsync(BrokerSubscription broker, CancellationToken cancellationToken)
    {
        var types = Acceptance(broker);
        if (types.Length == 0)
            return;

        await _connector.EnsureSubscriptionAsync(broker.Topic, broker.Name, types, cancellationToken)
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

    private async Task HandleDeliveryAsync(BrokerSubscription broker, ProcessMessageEventArgs args)
    {
        var body = args.Message.Body.ToMemory();
        var properties = args.Message.ApplicationProperties;
        int completed;
        try
        {
            completed = AzureServiceBusHeaders.CompletedAttempts(properties);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Azure Service Bus could not read the attempt count on {Topic}.", broker.Topic);
            completed = 0;
        }

        var attempt = completed + 1;
        var declaredName = ResolveDeclaredName(broker, properties);
        properties.TryGetValue(MessageHeaders.MessageId, out var rawMessageId);
        var messageType = declaredName ?? broker.Topic;
        using var consume = MessagingTelemetry.StartConsume(messageType, rawMessageId as string);
        consume?.SetTag("messaging.servicebus.topic", broker.Topic);
        consume?.SetTag("messaging.servicebus.subscription", broker.Name);
        consume?.SetTag("messaging.servicebus.delivery_count", args.Message.DeliveryCount);
        consume?.SetTag("messaging.servicebus.redelivered", completed > 0 || args.Message.DeliveryCount > 1);
        var started = TimeProvider.System.GetTimestamp();
        var settled = false;

        if (declaredName is null)
        {
            await FailAsync(
                broker,
                messageType,
                args,
                body.ToArray(),
                new MessageContext { TransportType = TransportTypeName },
                attempt,
                new InvalidOperationException(
                    "The message did not include lawndart_type, and the subscription handles more than one event type."),
                consume).ConfigureAwait(false);
            return;
        }

        TypeHandlers? typeHandlers;
        lock (broker.Gate)
            broker.Types.TryGetValue(declaredName, out typeHandlers);

        if (typeHandlers is null)
        {
            if (IsAccepted(broker, declaredName))
            {
                _logger.LogDebug(
                    "No handler is registered yet for {MessageType} on {Topic}/{Subscription}. The message returns after {Delay}.",
                    declaredName,
                    broker.Topic,
                    broker.Name,
                    _options.RetryDelay);
                await DelayRedeliveryAsync(broker, declaredName, args, completed).ConfigureAwait(false);
                return;
            }

            await FailAsync(
                broker,
                declaredName,
                args,
                body.ToArray(),
                new MessageContext { TransportType = TransportTypeName },
                attempt,
                new InvalidOperationException($"No handler is registered for '{declaredName}'."),
                consume).ConfigureAwait(false);
            return;
        }

        MessageEnvelope envelope;
        try
        {
            envelope = MessageEnvelopeHeaders.Read(
                AzureServiceBusHeaders.ToEnvelope(properties),
                TransportTypeName);
        }
        catch (Exception ex)
        {
            await FailAsync(
                broker,
                declaredName,
                args,
                body.ToArray(),
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
                _logger.LogDebug(
                    "No handler is registered for {MessageType} on {Topic}/{Subscription}. The message returns after {Delay}.",
                    declaredName,
                    broker.Topic,
                    broker.Name,
                    _options.RetryDelay);
                await DelayRedeliveryAsync(broker, declaredName, args, completed).ConfigureAwait(false);
                settled = true;
                return;
            }

            object? message;
            try
            {
                message = JsonSerializer.Deserialize(body.Span, typeHandlers.MessageType, MessagingJson.Options);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await FailAsync(
                    broker,
                    declaredName,
                    args,
                    body.ToArray(),
                    envelope.Context,
                    attempt,
                    ex,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            if (message is null)
            {
                await FailAsync(
                    broker,
                    declaredName,
                    args,
                    body.ToArray(),
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
                    await DelayRedeliveryAsync(broker, declaredName, args, completed).ConfigureAwait(false);
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
                    await DelayRedeliveryAsync(broker, declaredName, args, completed).ConfigureAwait(false);
                    settled = true;
                    return;
                }

                if (result.Succeeded)
                    continue;

                await FailAsync(
                    broker,
                    declaredName,
                    args,
                    message,
                    envelope.Context,
                    attempt,
                    result.Error!,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            using var timeout = LinkTimeout(CancellationToken.None);
            await args.CompleteMessageAsync(args.Message, timeout.Token).ConfigureAwait(false);
            settled = true;
            MessagingTelemetry.RecordConsume(declaredName, TimeProvider.System.GetElapsedTime(started));
        }
        catch (Exception ex)
        {
            consume?.SetStatus(ActivityStatusCode.Error, ex.Message);
            _logger.LogError(
                ex,
                "Azure Service Bus consumer failed while handling {MessageType} on {Topic}/{Subscription}",
                declaredName,
                broker.Topic,
                broker.Name);
            if (!settled)
            {
                try
                {
                    await DelayRedeliveryAsync(broker, declaredName, args, attempt).ConfigureAwait(false);
                }
                catch (Exception settleEx)
                {
                    _logger.LogError(
                        settleEx,
                        "Azure Service Bus could not reschedule {MessageType} on {Topic}/{Subscription}.",
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
        ProcessMessageEventArgs args,
        object message,
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

            await DelayRedeliveryAsync(broker, declaredName, args, completedAttempts: attempt).ConfigureAwait(false);
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
        _deadLetters.Enqueue(new DeadLetteredMessage(
            declaredName,
            message,
            context,
            attempt,
            errorText));

        using var timeout = LinkTimeout(CancellationToken.None);
        await args.DeadLetterMessageAsync(
            args.Message,
            Trim(errorType, 4096),
            Trim(errorText, 4096),
            timeout.Token).ConfigureAwait(false);
    }

    private async Task DelayRedeliveryAsync(
        BrokerSubscription broker,
        string declaredName,
        ProcessMessageEventArgs args,
        int completedAttempts)
    {
        var scheduled = new ServiceBusMessage(args.Message.Body)
        {
            ContentType = string.IsNullOrEmpty(args.Message.ContentType) ? "application/json" : args.Message.ContentType,
            CorrelationId = args.Message.CorrelationId,
            Subject = string.IsNullOrEmpty(args.Message.Subject) ? declaredName : args.Message.Subject,
            MessageId = args.Message.MessageId,
        };

        foreach (var pair in args.Message.ApplicationProperties)
        {
            if (pair.Key is AzureServiceBusHeaders.Attempt or AzureServiceBusHeaders.TargetSubscription or AzureServiceBusHeaders.EventType || pair.Value is null)
                continue;
            scheduled.ApplicationProperties[pair.Key] = pair.Value;
        }

        scheduled.ApplicationProperties[AzureServiceBusHeaders.Attempt] = completedAttempts;
        scheduled.ApplicationProperties[AzureServiceBusHeaders.TargetSubscription] = broker.Name;
        scheduled.ApplicationProperties[AzureServiceBusHeaders.EventType] = declaredName;
        var enqueueAt = DateTimeOffset.UtcNow.Add(_options.RetryDelay);
        var sender = await _connector.SenderAsync(broker.Topic, CancellationToken.None).ConfigureAwait(false);
        using var timeout = LinkTimeout(CancellationToken.None);
        await sender.ScheduleMessageAsync(scheduled, enqueueAt, timeout.Token).ConfigureAwait(false);
        await args.CompleteMessageAsync(args.Message, timeout.Token).ConfigureAwait(false);
    }

    private static ServiceBusMessage CreateOutbound(byte[] body, MessageContext context, string declaredName)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        MessageEnvelopeHeaders.Write(context, declaredName, headers);

        var brokerMessage = new ServiceBusMessage(body)
        {
            ContentType = "application/json",
            Subject = declaredName,
        };

        if (!string.IsNullOrWhiteSpace(context.MessageId) && context.MessageId.Length <= 128)
            brokerMessage.MessageId = context.MessageId;
        if (!string.IsNullOrWhiteSpace(context.CorrelationId) && context.CorrelationId.Length <= 128)
            brokerMessage.CorrelationId = context.CorrelationId;

        foreach (var pair in headers)
            brokerMessage.ApplicationProperties[pair.Key] = pair.Value;

        brokerMessage.ApplicationProperties[AzureServiceBusHeaders.TargetSubscription] = AzureServiceBusHeaders.TargetAll;
        brokerMessage.ApplicationProperties[AzureServiceBusHeaders.EventType] = declaredName;
        return brokerMessage;
    }

    private static string? ResolveDeclaredName(
        BrokerSubscription broker,
        IReadOnlyDictionary<string, object> properties)
    {
        var named = ReadString(properties, AzureServiceBusHeaders.EventType)
            ?? ReadString(properties, MessageHeaders.MessageType);

        lock (broker.Gate)
        {
            if (named is not null && broker.Types.ContainsKey(named))
                return named;

            if (named is null && broker.Types.Count == 1)
            {
                foreach (var key in broker.Types.Keys)
                    return key;
            }
        }

        return named;
    }

    private static string? ReadString(IReadOnlyDictionary<string, object> properties, string key)
    {
        if (!properties.TryGetValue(key, out var value) || value is not string text || text.Length == 0)
            return null;

        return text;
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
            return broker.AcceptedTypes.Contains(declaredName);
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

        await StopProcessorAsync(broker).ConfigureAwait(false);
        lock (broker.Gate)
            broker.Startup = null;
    }

    private async Task StopProcessorAsync(BrokerSubscription broker)
    {
        var processor = broker.Processor;
        if (processor is null)
            return;

        broker.Processor = null;
        using var timeout = new CancellationTokenSource(_options.OperationTimeout);
        try
        {
            if (processor.IsProcessing)
                await processor.StopProcessingAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Azure Service Bus processor stop failed for {Topic}/{Subscription}.", broker.Topic, broker.Name);
        }

        try
        {
            await processor.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Azure Service Bus processor dispose failed for {Topic}/{Subscription}.", broker.Topic, broker.Name);
        }
    }

    private CancellationTokenSource LinkTimeout(CancellationToken cancellationToken)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(_options.OperationTimeout);
        return source;
    }

    private static string Trim(string value, int max)
        => value.Length <= max ? value : value[..max];

    private sealed class BrokerSubscription
    {
        public BrokerSubscription(string topic, string name)
        {
            Topic = topic;
            Name = name;
        }

        public string Topic { get; }

        public string Name { get; }

        public Dictionary<string, TypeHandlers> Types { get; } = new(StringComparer.Ordinal);

        public HashSet<string> AcceptedTypes { get; } = new(StringComparer.Ordinal);

        public object Gate { get; } = new();

        public Task? Startup { get; set; }

        public ServiceBusProcessor? Processor { get; set; }
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
