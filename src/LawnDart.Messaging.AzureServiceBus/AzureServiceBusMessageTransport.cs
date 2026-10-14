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
/// The topic is the declared message type name. Each subscriber has one subscription on that topic.
/// Publish creates the topic. A subscription creates its own subscription entity.
/// A failed handler is scheduled again after <see cref="AzureServiceBusTransportOptions.RetryDelay"/>
/// until <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
/// When dead-letter handling is on, the last failure is dead-lettered on the subscription.
/// When it is off, the message is scheduled again and stays available.
/// A cancelled delivery uses the same wait. It is not abandoned immediately.
/// A retry sets <c>lawndart_target</c> to that subscription.
/// A publish sets it to <c>*</c>. A message with no <c>lawndart_target</c> reaches every subscription.
/// Scheduling the retry and then completing the original can leave both copies
/// when the complete fails after the schedule succeeds.
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
    private readonly ConcurrentDictionary<Type, Subscription> _subscriptions = new();
    private int _disposed;

    /// <summary>
    /// Creates a transport that opens its own clients from <paramref name="options"/>.
    /// </summary>
    /// <param name="logger">Logger. A warning is written when a message is dead-lettered.</param>
    /// <param name="options">Connection, prefetch, retry, and dead-letter settings.</param>
    /// <param name="preHandlers">Hooks invoked before each handler attempt. May be empty.</param>
    /// <param name="deadLetters">In-process copy of messages dead-lettered on the subscription. A memory queue is used when omitted.</param>
    public AzureServiceBusMessageTransport(
        ILogger<AzureServiceBusMessageTransport> logger,
        IOptions<AzureServiceBusTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers = null,
        IDeadLetterQueue? deadLetters = null)
        : this(logger, options, preHandlers, deadLetters, CreateOwnedConnector(options), ownsConnector: true)
    {
    }

    internal AzureServiceBusMessageTransport(
        ILogger<AzureServiceBusMessageTransport> logger,
        IOptions<AzureServiceBusTransportOptions> options,
        IEnumerable<IMessagePreHandler>? preHandlers,
        IDeadLetterQueue? deadLetters,
        AzureServiceBusConnector connector,
        bool ownsConnector)
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
    }

    private static AzureServiceBusConnector CreateOwnedConnector(IOptions<AzureServiceBusTransportOptions> options)
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
            ownsAdmin: true);
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
        var topic = AzureServiceBusNames.Topic(declaredName);
        cancellationToken.ThrowIfCancellationRequested();

        using var publish = MessagingTelemetry.StartPublish(declaredName);
        publish?.SetTag("messaging.servicebus.topic", topic);
        var started = TimeProvider.System.GetTimestamp();

        try
        {
            await WaitForInFlightStartupAsync<TMessage>(cancellationToken).ConfigureAwait(false);
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
        var topic = AzureServiceBusNames.Topic(declaredName);
        var subscription = _subscriptions.GetOrAdd(
            typeof(TMessage),
            _ => new Subscription(declaredName, topic, _options.SubscriptionName));

        var registration = new HandlerRegistration(handler, cancellationToken);
        lock (subscription.Handlers)
            subscription.Handlers.Add(registration);

        using var startupTimeout = new CancellationTokenSource(_options.OperationTimeout);
        try
        {
            await EnsureConsumerAsync<TMessage>(subscription, startupTimeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (startupTimeout.IsCancellationRequested && ex is OperationCanceledException or TimeoutException)
        {
            await AbortStartupAsync(subscription).ConfigureAwait(false);
            lock (subscription.Handlers)
                subscription.Handlers.Remove(registration);
            throw new TimeoutException(
                $"Azure Service Bus subscription for '{declaredName}' did not start within {_options.OperationTimeout}.",
                ex);
        }
        catch
        {
            lock (subscription.Handlers)
                subscription.Handlers.Remove(registration);
            throw;
        }

        if (!cancellationToken.CanBeCanceled)
            return;

        var unregister = cancellationToken.Register(() =>
        {
            lock (subscription.Handlers)
                subscription.Handlers.Remove(registration);
            _ = StopThenCompleteAsync(subscription, registration);
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
        if (!_subscriptions.TryGetValue(typeof(TMessage), out var subscription))
            return;

        lock (subscription.Handlers)
            subscription.Handlers.Clear();
    }

    /// <summary>Closes processors and the clients this transport opened.</summary>
    public void Dispose()
    {
        var dispose = DisposeAsync().AsTask();
        if (!dispose.Wait(_options.OperationTimeout))
        {
            _logger.LogWarning(
                "Disposing the Azure Service Bus transport did not finish within {Timeout}.",
                _options.OperationTimeout);
        }
    }

    /// <summary>Closes processors and the clients this transport opened.</summary>
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

            await StopProcessorAsync(subscription).ConfigureAwait(false);
            foreach (var handler in handlers)
                handler.Completed.TrySetResult();
        }

        if (_ownsConnector)
            await _connector.DisposeAsync().ConfigureAwait(false);
    }

    private async Task EnsureConsumerAsync<TMessage>(Subscription subscription, CancellationToken cancellationToken)
        where TMessage : class
    {
        Task startup;
        lock (subscription)
        {
            subscription.Startup ??= StartConsumerAsync<TMessage>(subscription, cancellationToken);
            startup = subscription.Startup;
        }

        try
        {
            await startup.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await ObserveStartupAsync(startup).ConfigureAwait(false);
            await AbortStartupAsync(subscription).ConfigureAwait(false);
            throw;
        }
        catch
        {
            await AbortStartupAsync(subscription).ConfigureAwait(false);
            throw;
        }
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

    private async Task AbortStartupAsync(Subscription subscription)
    {
        await StopProcessorAsync(subscription).ConfigureAwait(false);
        lock (subscription)
            subscription.Startup = null;
    }

    private async Task StartConsumerAsync<TMessage>(Subscription subscription, CancellationToken cancellationToken)
        where TMessage : class
    {
        await _connector.EnsureSubscriptionAsync(subscription.Topic, subscription.Name, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        var processor = _connector.Client.CreateProcessor(
            subscription.Topic,
            subscription.Name,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = _options.Concurrency,
                PrefetchCount = _options.Prefetch,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                Identifier = subscription.Name,
            });

        processor.ProcessMessageAsync += args => HandleDeliveryAsync<TMessage>(subscription, args);
        processor.ProcessErrorAsync += args =>
        {
            _logger.LogError(
                args.Exception,
                "Azure Service Bus processor failed for {Topic}/{Subscription}. Source {ErrorSource}.",
                subscription.Topic,
                subscription.Name,
                args.ErrorSource);
            return Task.CompletedTask;
        };

        try
        {
            await processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            subscription.Processor = processor;
        }
        catch
        {
            try
            {
                await processor.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception disposeEx)
            {
                _logger.LogDebug(disposeEx, "Azure Service Bus processor dispose failed for {Topic}/{Subscription}.", subscription.Topic, subscription.Name);
            }

            throw;
        }

        var idle = false;
        lock (subscription.Handlers)
            idle = subscription.Handlers.Count == 0;

        if (idle)
            await StopIfIdleAsync(subscription).ConfigureAwait(false);
    }

    private async Task WaitForInFlightStartupAsync<TMessage>(CancellationToken cancellationToken)
        where TMessage : class
    {
        if (!_subscriptions.TryGetValue(typeof(TMessage), out var subscription))
            return;

        Task? startup;
        lock (subscription)
            startup = subscription.Startup;

        if (startup is null || startup.IsCompleted)
            return;

        await startup.WaitAsync(_options.OperationTimeout, cancellationToken).ConfigureAwait(false);
    }

    private async Task HandleDeliveryAsync<TMessage>(Subscription subscription, ProcessMessageEventArgs args)
        where TMessage : class
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
            _logger.LogError(ex, "Azure Service Bus could not read the attempt count on {Topic}.", subscription.Topic);
            completed = 0;
        }

        var attempt = completed + 1;
        properties.TryGetValue(MessageHeaders.MessageId, out var rawMessageId);
        using var consume = MessagingTelemetry.StartConsume(subscription.DeclaredName, rawMessageId as string);
        consume?.SetTag("messaging.servicebus.topic", subscription.Topic);
        consume?.SetTag("messaging.servicebus.subscription", subscription.Name);
        consume?.SetTag("messaging.servicebus.delivery_count", args.Message.DeliveryCount);
        consume?.SetTag("messaging.servicebus.redelivered", completed > 0 || args.Message.DeliveryCount > 1);
        var started = TimeProvider.System.GetTimestamp();
        var settled = false;

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
                subscription,
                args,
                body,
                new MessageContext { TransportType = TransportTypeName },
                body.ToArray(),
                attempt,
                ex,
                consume).ConfigureAwait(false);
            return;
        }

        try
        {
            List<HandlerRegistration> handlers;
            lock (subscription.Handlers)
                handlers = [.. subscription.Handlers];

            if (handlers.Count == 0)
            {
                _logger.LogDebug(
                    "No handler is registered for {MessageType} on {Topic}/{Subscription}. The message returns after {Delay}.",
                    subscription.DeclaredName,
                    subscription.Topic,
                    subscription.Name,
                    _options.RetryDelay);
                await DelayRedeliveryAsync(subscription, args, completed).ConfigureAwait(false);
                settled = true;
                return;
            }

            TMessage? message;
            try
            {
                message = JsonSerializer.Deserialize<TMessage>(body.Span, MessagingJson.Options);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await FailAsync(
                    subscription,
                    args,
                    body,
                    envelope.Context,
                    body.ToArray(),
                    attempt,
                    ex,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            if (message is null)
            {
                await FailAsync(
                    subscription,
                    args,
                    body,
                    envelope.Context,
                    body.ToArray(),
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
                    await DelayRedeliveryAsync(subscription, args, completed).ConfigureAwait(false);
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
                    await DelayRedeliveryAsync(subscription, args, completed).ConfigureAwait(false);
                    settled = true;
                    return;
                }

                if (result.Succeeded)
                    continue;

                await FailAsync(
                    subscription,
                    args,
                    message,
                    envelope.Context,
                    body,
                    attempt,
                    result.Error!,
                    consume).ConfigureAwait(false);
                settled = true;
                return;
            }

            using var timeout = LinkTimeout(CancellationToken.None);
            await args.CompleteMessageAsync(args.Message, timeout.Token).ConfigureAwait(false);
            settled = true;
            MessagingTelemetry.RecordConsume(subscription.DeclaredName, TimeProvider.System.GetElapsedTime(started));
        }
        catch (Exception ex)
        {
            consume?.SetStatus(ActivityStatusCode.Error, ex.Message);
            _logger.LogError(
                ex,
                "Azure Service Bus consumer failed while handling {MessageType} on {Topic}/{Subscription}",
                subscription.DeclaredName,
                subscription.Topic,
                subscription.Name);
            if (!settled)
            {
                try
                {
                    await DelayRedeliveryAsync(subscription, args, attempt).ConfigureAwait(false);
                }
                catch (Exception settleEx)
                {
                    _logger.LogError(
                        settleEx,
                        "Azure Service Bus could not reschedule {MessageType} on {Topic}/{Subscription}.",
                        subscription.DeclaredName,
                        subscription.Topic,
                        subscription.Name);
                }
            }
        }
    }

    private async Task FailAsync(
        Subscription subscription,
        ProcessMessageEventArgs args,
        object message,
        MessageContext context,
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

            await DelayRedeliveryAsync(subscription, args, completedAttempts: attempt).ConfigureAwait(false);
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

        using var timeout = LinkTimeout(CancellationToken.None);
        await args.DeadLetterMessageAsync(
            args.Message,
            Trim(errorType, 4096),
            Trim(errorText, 4096),
            timeout.Token).ConfigureAwait(false);
    }

    private async Task DelayRedeliveryAsync(
        Subscription subscription,
        ProcessMessageEventArgs args,
        int completedAttempts)
    {
        var scheduled = new ServiceBusMessage(args.Message.Body)
        {
            ContentType = string.IsNullOrEmpty(args.Message.ContentType) ? "application/json" : args.Message.ContentType,
            CorrelationId = args.Message.CorrelationId,
            Subject = string.IsNullOrEmpty(args.Message.Subject) ? subscription.DeclaredName : args.Message.Subject,
            MessageId = args.Message.MessageId,
        };

        foreach (var pair in args.Message.ApplicationProperties)
        {
            if (pair.Key is AzureServiceBusHeaders.Attempt or AzureServiceBusHeaders.TargetSubscription || pair.Value is null)
                continue;
            scheduled.ApplicationProperties[pair.Key] = pair.Value;
        }

        scheduled.ApplicationProperties[AzureServiceBusHeaders.Attempt] = completedAttempts;
        scheduled.ApplicationProperties[AzureServiceBusHeaders.TargetSubscription] = subscription.Name;
        var enqueueAt = DateTimeOffset.UtcNow.Add(_options.RetryDelay);
        var sender = await _connector.SenderAsync(subscription.Topic, CancellationToken.None).ConfigureAwait(false);
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
        return brokerMessage;
    }

    private async Task StopThenCompleteAsync(Subscription subscription, HandlerRegistration registration)
    {
        try
        {
            await StopIfIdleAsync(subscription).ConfigureAwait(false);
        }
        finally
        {
            registration.Completed.TrySetResult();
        }
    }

    private async Task StopIfIdleAsync(Subscription subscription)
    {
        lock (subscription.Handlers)
        {
            if (subscription.Handlers.Count > 0)
                return;
        }

        await StopProcessorAsync(subscription).ConfigureAwait(false);
        lock (subscription)
            subscription.Startup = null;
    }

    private async Task StopProcessorAsync(Subscription subscription)
    {
        var processor = subscription.Processor;
        if (processor is null)
            return;

        subscription.Processor = null;
        using var timeout = new CancellationTokenSource(_options.OperationTimeout);
        try
        {
            if (processor.IsProcessing)
                await processor.StopProcessingAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Azure Service Bus processor stop failed for {Topic}/{Subscription}.", subscription.Topic, subscription.Name);
        }

        try
        {
            await processor.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Azure Service Bus processor dispose failed for {Topic}/{Subscription}.", subscription.Topic, subscription.Name);
        }
    }

    private CancellationTokenSource LinkTimeout(CancellationToken cancellationToken)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(_options.OperationTimeout);
        return source;
    }

    private static async Task WaitUntilCancelledAsync(
        HandlerRegistration registration,
        CancellationTokenRegistration unregister)
    {
        try
        {
            await registration.Completed.Task.ConfigureAwait(false);
        }
        finally
        {
            await unregister.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static string Trim(string value, int max)
        => value.Length <= max ? value : value[..max];

    private sealed class Subscription
    {
        public Subscription(string declaredName, string topic, string name)
        {
            DeclaredName = declaredName;
            Topic = topic;
            Name = name;
        }

        public string DeclaredName { get; }

        public string Topic { get; }

        public string Name { get; }

        public List<HandlerRegistration> Handlers { get; } = [];

        public Task? Startup { get; set; }

        public ServiceBusProcessor? Processor { get; set; }
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
