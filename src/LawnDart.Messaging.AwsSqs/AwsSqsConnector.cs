using System.Net;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Shared SQS and SNS clients, and topic, queue, and subscription declarations.
/// </summary>
internal sealed class AwsSqsConnector : IDisposable, IAsyncDisposable
{
    private const int DeclareAttempts = 8;
    private readonly AwsSqsTransportOptions _options;
    private readonly Func<IAmazonSQS> _sqsFactory;
    private readonly Func<IAmazonSimpleNotificationService> _snsFactory;
    private readonly bool _ownsClients;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly SemaphoreSlim _declareLock = new(1, 1);
    private readonly Dictionary<string, string> _topicArns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, QueueHandle> _queues = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _subscriptions = new(StringComparer.Ordinal);
    private readonly HashSet<string> _ready = new(StringComparer.Ordinal);
    private readonly HashSet<string> _routable = new(StringComparer.Ordinal);
    private readonly ILogger _logger;
    private IAmazonSQS? _sqs;
    private IAmazonSimpleNotificationService? _sns;
    private int _disposed;

    public AwsSqsConnector(
        AwsSqsTransportOptions options,
        Func<IAmazonSQS> sqsFactory,
        Func<IAmazonSimpleNotificationService> snsFactory,
        bool ownsClients,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(sqsFactory);
        ArgumentNullException.ThrowIfNull(snsFactory);
        _options = options;
        _sqsFactory = sqsFactory;
        _snsFactory = snsFactory;
        _ownsClients = ownsClients;
        _logger = logger ?? NullLogger.Instance;
    }

    public bool IsRoutable(string topic)
    {
        lock (_routable)
            return _routable.Contains(topic);
    }

    public void MarkRoutable(string topic)
    {
        lock (_routable)
            _routable.Add(topic);
    }

    public async Task<string> EnsureTopicAsync(string topic, CancellationToken cancellationToken)
    {
        var cached = TopicArn(topic);
        if (cached is not null)
            return cached;

        await DeclareOnceAsync(
            "topic\n" + topic,
            async ct =>
            {
                var response = await CallAsync(
                    token => Sns.CreateTopicAsync(new CreateTopicRequest { Name = topic }, token),
                    ct).ConfigureAwait(false);
                RememberTopic(topic, response.TopicArn);
            },
            cancellationToken).ConfigureAwait(false);

        return TopicArn(topic)
            ?? throw new InvalidOperationException($"SNS did not return an ARN for topic '{topic}'.");
    }

    public async Task<bool> HasQueueSubscriptionAsync(string topic, CancellationToken cancellationToken)
    {
        var topicArn = await EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);
        string? token = null;
        do
        {
            var page = await CallAsync(
                ct => Sns.ListSubscriptionsByTopicAsync(
                    new ListSubscriptionsByTopicRequest { TopicArn = topicArn, NextToken = token },
                    ct),
                cancellationToken).ConfigureAwait(false);
            foreach (var subscription in page.Subscriptions ?? [])
            {
                if (IsLiveQueue(subscription))
                    return true;
            }

            token = page.NextToken;
        }
        while (!string.IsNullOrEmpty(token));

        return false;
    }

    public async Task<string> EnsureSubscriptionAsync(
        string topic,
        string subscriptionName,
        IReadOnlyCollection<string> messageTypes,
        bool replaceRouteRule,
        bool createDeadLetter,
        CancellationToken cancellationToken)
    {
        var queueName = AwsSqsNames.Queue(topic, subscriptionName);
        var requested = Normalize(messageTypes);
        var key = "subscription\n" + topic + "\n" + queueName + "\n" +
                  string.Join("\n", requested) + (replaceRouteRule ? "\nreplace" : "") +
                  (createDeadLetter ? "\ndlq\n" + _options.MaxReceiveCount.ToString(System.Globalization.CultureInfo.InvariantCulture) : "");

        await DeclareOnceAsync(
            key,
            async ct =>
            {
                var topicArn = await CreateTopicAsync(topic, ct).ConfigureAwait(false);
                QueueHandle? deadLetter = null;
                if (createDeadLetter)
                    deadLetter = await EnsureQueueAsync(AwsSqsNames.DeadLetterQueue(topic, subscriptionName), ct).ConfigureAwait(false);

                var queue = await EnsureQueueAsync(queueName, ct).ConfigureAwait(false);
                await SetQueuePolicyAsync(queue, topicArn, ct).ConfigureAwait(false);
                if (deadLetter is not null)
                    await SetRedriveAsync(queue, deadLetter.Arn, ct).ConfigureAwait(false);
                var initialFilter = requested.Count == 0 ? null : AwsSqsFilterPolicy.Build(requested);
                var subscriptionArn = await EnsureOneSubscriptionAsync(topicArn, queue.Arn, initialFilter, ct).ConfigureAwait(false);
                await EnsureRawDeliveryAsync(subscriptionArn, ct).ConfigureAwait(false);
                if (requested.Count > 0)
                    await EnsureFilterAsync(subscriptionArn, requested, replaceRouteRule, ct).ConfigureAwait(false);

                MarkRoutable(topic);
            },
            cancellationToken).ConfigureAwait(false);

        return QueueUrl(queueName)
            ?? throw new InvalidOperationException($"SQS did not return a URL for queue '{queueName}'.");
    }

    public async Task<string> EnsureDeadLetterUrlAsync(
        string topic,
        string subscriptionName,
        CancellationToken cancellationToken)
    {
        var name = AwsSqsNames.DeadLetterQueue(topic, subscriptionName);
        var cached = FindQueue(name);
        if (cached is not null)
            return cached.Url;

        await DeclareOnceAsync(
            "dlq\n" + name,
            ct => EnsureQueueAsync(name, ct),
            cancellationToken).ConfigureAwait(false);

        return FindQueue(name)?.Url
            ?? throw new InvalidOperationException($"SQS did not return a URL for queue '{name}'.");
    }

    public Task PublishAsync(
        string topicArn,
        string body,
        IReadOnlyDictionary<string, Amazon.SQS.Model.MessageAttributeValue> attributes,
        CancellationToken cancellationToken)
        => CallAsync(
            ct => Sns.PublishAsync(
                new PublishRequest
                {
                    TopicArn = topicArn,
                    Message = body,
                    MessageAttributes = ToSnsAttributes(attributes),
                },
                ct),
            cancellationToken);

    public Task SendAsync(
        string queueUrl,
        string body,
        Dictionary<string, Amazon.SQS.Model.MessageAttributeValue> attributes,
        int delaySeconds,
        CancellationToken cancellationToken)
        => CallAsync(
            ct => Sqs.SendMessageAsync(
                new SendMessageRequest
                {
                    QueueUrl = queueUrl,
                    MessageBody = body,
                    DelaySeconds = delaySeconds,
                    MessageAttributes = attributes,
                },
                ct),
            cancellationToken);

    public Task ChangeVisibilityAsync(
        string queueUrl,
        string receiptHandle,
        int visibilitySeconds,
        CancellationToken cancellationToken)
        => CallAsync(
            ct => Sqs.ChangeMessageVisibilityAsync(
                new ChangeMessageVisibilityRequest
                {
                    QueueUrl = queueUrl,
                    ReceiptHandle = receiptHandle,
                    VisibilityTimeout = visibilitySeconds,
                },
                ct),
            cancellationToken);

    public async Task<IReadOnlyList<Message>> ReceiveAsync(
        string queueUrl,
        int maxMessages,
        int waitSeconds,
        int visibilitySeconds,
        CancellationToken cancellationToken)
    {
        var response = await CallAsync(
            ct => Sqs.ReceiveMessageAsync(
                new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    MaxNumberOfMessages = maxMessages,
                    WaitTimeSeconds = waitSeconds,
                    VisibilityTimeout = visibilitySeconds,
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = ["ApproximateReceiveCount"],
                },
                ct),
            cancellationToken).ConfigureAwait(false);
        return response.Messages ?? [];
    }

    public Task DeleteAsync(string queueUrl, string receiptHandle, CancellationToken cancellationToken)
        => CallAsync(
            ct => Sqs.DeleteMessageAsync(
                new DeleteMessageRequest { QueueUrl = queueUrl, ReceiptHandle = receiptHandle },
                ct),
            cancellationToken);

    public void Dispose()
    {
        var dispose = Task.Run(async () => await DisposeAsync().ConfigureAwait(false));
        if (!dispose.Wait(_options.OperationTimeout))
        {
            _logger.LogWarning(
                "Disposing the AWS SQS connector did not finish within {Timeout}. Prefer DisposeAsync.",
                _options.OperationTimeout);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (_ownsClients)
        {
            await DisposeClientAsync(_sqs).ConfigureAwait(false);
            await DisposeClientAsync(_sns).ConfigureAwait(false);
        }

        _initLock.Dispose();
        _declareLock.Dispose();
    }

    private async Task<string> CreateTopicAsync(string topic, CancellationToken cancellationToken)
    {
        var cached = TopicArn(topic);
        if (cached is not null)
            return cached;

        var response = await CallAsync(
            ct => Sns.CreateTopicAsync(new CreateTopicRequest { Name = topic }, ct),
            cancellationToken).ConfigureAwait(false);
        RememberTopic(topic, response.TopicArn);
        return response.TopicArn;
    }

    private async Task<QueueHandle> EnsureQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        var cached = FindQueue(queueName);
        if (cached is not null)
            return cached;

        string url;
        try
        {
            var created = await CallAsync(
                ct => Sqs.CreateQueueAsync(
                    new CreateQueueRequest
                    {
                        QueueName = queueName,
                        Attributes = QueueAttributes(),
                    },
                    ct),
                cancellationToken).ConfigureAwait(false);
            url = created.QueueUrl;
        }
        catch (Exception ex) when (IsAlreadyExists(ex))
        {
            var existing = await CallAsync(
                ct => Sqs.GetQueueUrlAsync(new GetQueueUrlRequest { QueueName = queueName }, ct),
                cancellationToken).ConfigureAwait(false);
            url = existing.QueueUrl;
            await CallAsync(
                ct => Sqs.SetQueueAttributesAsync(
                    new SetQueueAttributesRequest
                    {
                        QueueUrl = url,
                        Attributes = QueueAttributes(),
                    },
                    ct),
                cancellationToken).ConfigureAwait(false);
        }

        var attributes = await CallAsync(
            ct => Sqs.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = url,
                    AttributeNames = ["QueueArn"],
                },
                ct),
            cancellationToken).ConfigureAwait(false);
        if (attributes.Attributes is null ||
            !attributes.Attributes.TryGetValue("QueueArn", out var arn) ||
            string.IsNullOrWhiteSpace(arn))
        {
            throw new InvalidOperationException($"SQS did not return an ARN for queue '{queueName}'.");
        }

        var handle = new QueueHandle(url, arn);
        lock (_queues)
            _queues[queueName] = handle;
        return handle;
    }

    private Dictionary<string, string> QueueAttributes()
    {
        var visibility = WholeSeconds(_options.VisibilityTimeout, 1, AwsSqsTransportOptions.MaxVisibilitySeconds);
        var wait = WholeSeconds(_options.LongPollWait, 0, AwsSqsTransportOptions.MaxLongPollSeconds);
        var receiveBudget = (int)_options.OperationTimeout.TotalSeconds;
        if (receiveBudget >= 0 && wait > receiveBudget)
            wait = receiveBudget;

        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["VisibilityTimeout"] = visibility.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["ReceiveMessageWaitTimeSeconds"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
    }

    private async Task SetQueuePolicyAsync(QueueHandle queue, string topicArn, CancellationToken cancellationToken)
    {
        var policy = AllowSns(queue.Arn, topicArn);
        await CallAsync(
            ct => Sqs.SetQueueAttributesAsync(
                new SetQueueAttributesRequest
                {
                    QueueUrl = queue.Url,
                    Attributes = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["Policy"] = policy,
                    },
                },
                ct),
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> EnsureOneSubscriptionAsync(
        string topicArn,
        string queueArn,
        string? initialFilter,
        CancellationToken cancellationToken)
    {
        var cacheKey = topicArn + "\n" + queueArn;
        var cached = SubscriptionArn(cacheKey);
        if (cached is not null)
            return cached;

        for (var attempt = 1; attempt <= DeclareAttempts; attempt++)
        {
            var found = await ListQueueSubscriptionsAsync(topicArn, queueArn, cancellationToken).ConfigureAwait(false);
            if (found.Count == 1)
            {
                RememberSubscription(cacheKey, found[0]);
                return found[0];
            }

            if (found.Count == 0)
            {
                try
                {
                    await CallAsync(
                        ct => Sns.SubscribeAsync(
                            new SubscribeRequest
                            {
                                TopicArn = topicArn,
                                Protocol = "sqs",
                                Endpoint = queueArn,
                                ReturnSubscriptionArn = true,
                                Attributes = SubscribeAttributes(initialFilter),
                            },
                            ct),
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsAlreadyExists(ex) || IsSubscriptionAttributeRace(ex))
                {
                }
            }
            else
            {
                var keep = found.OrderBy(static arn => arn, StringComparer.Ordinal).First();
                foreach (var extra in found)
                {
                    if (string.Equals(extra, keep, StringComparison.Ordinal))
                        continue;
                    try
                    {
                        await CallAsync(
                            ct => Sns.UnsubscribeAsync(new UnsubscribeRequest { SubscriptionArn = extra }, ct),
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (IsNotFound(ex))
                    {
                    }
                }
            }

            if (attempt < DeclareAttempts)
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"SNS could not settle one subscription from '{topicArn}' to '{queueArn}' after {DeclareAttempts} tries.");
    }

    private async Task<List<string>> ListQueueSubscriptionsAsync(
        string topicArn,
        string queueArn,
        CancellationToken cancellationToken)
    {
        var found = new List<string>();
        string? token = null;
        do
        {
            var page = await CallAsync(
                ct => Sns.ListSubscriptionsByTopicAsync(
                    new ListSubscriptionsByTopicRequest { TopicArn = topicArn, NextToken = token },
                    ct),
                cancellationToken).ConfigureAwait(false);
            foreach (var subscription in page.Subscriptions ?? [])
            {
                if (!IsLiveQueue(subscription))
                    continue;
                if (!string.Equals(subscription.Endpoint, queueArn, StringComparison.Ordinal))
                    continue;
                found.Add(subscription.SubscriptionArn);
            }

            token = page.NextToken;
        }
        while (!string.IsNullOrEmpty(token));

        return found;
    }

    private async Task EnsureRawDeliveryAsync(string subscriptionArn, CancellationToken cancellationToken)
    {
        var attributes = await GetSubscriptionAttributesAsync(subscriptionArn, cancellationToken).ConfigureAwait(false);
        if (attributes.TryGetValue("RawMessageDelivery", out var raw) &&
            string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        await CallAsync(
            ct => Sns.SetSubscriptionAttributesAsync(
                new SetSubscriptionAttributesRequest
                {
                    SubscriptionArn = subscriptionArn,
                    AttributeName = "RawMessageDelivery",
                    AttributeValue = "true",
                },
                ct),
            cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureFilterAsync(
        string subscriptionArn,
        IReadOnlyList<string> requested,
        bool replaceRouteRule,
        CancellationToken cancellationToken)
    {
        const int confirmRounds = 3;
        for (var attempt = 1; attempt <= DeclareAttempts; attempt++)
        {
            var current = await ReadFilterAsync(subscriptionArn, cancellationToken).ConfigureAwait(false);
            if (AwsSqsFilterPolicy.Covers(current, requested, replaceRouteRule))
                return;

            var desired = AwsSqsFilterPolicy.Merge(current, requested, replaceRouteRule);
            await CallAsync(
                ct => Sns.SetSubscriptionAttributesAsync(
                    new SetSubscriptionAttributesRequest
                    {
                        SubscriptionArn = subscriptionArn,
                        AttributeName = "FilterPolicy",
                        AttributeValue = desired,
                    },
                    ct),
                cancellationToken).ConfigureAwait(false);
            await CallAsync(
                ct => Sns.SetSubscriptionAttributesAsync(
                    new SetSubscriptionAttributesRequest
                    {
                        SubscriptionArn = subscriptionArn,
                        AttributeName = "FilterPolicyScope",
                        AttributeValue = "MessageAttributes",
                    },
                    ct),
                cancellationToken).ConfigureAwait(false);

            var stable = true;
            for (var round = 1; round <= confirmRounds; round++)
            {
                await Task.Delay(FilterJitter(attempt, round), cancellationToken).ConfigureAwait(false);
                var readback = await ReadFilterAsync(subscriptionArn, cancellationToken).ConfigureAwait(false);
                if (AwsSqsFilterPolicy.Covers(readback, requested, replaceRouteRule))
                    continue;

                stable = false;
                break;
            }

            if (stable)
                return;
        }

        throw new InvalidOperationException(
            $"SNS could not update the filter policy for '{subscriptionArn}' after {DeclareAttempts} tries.");
    }

    private static TimeSpan FilterJitter(int attempt, int round)
    {
        var milliseconds = 20 * attempt + 15 * round + Random.Shared.Next(0, 40);
        return TimeSpan.FromMilliseconds(milliseconds);
    }

    private async Task<string?> ReadFilterAsync(string subscriptionArn, CancellationToken cancellationToken)
    {
        var attributes = await GetSubscriptionAttributesAsync(subscriptionArn, cancellationToken).ConfigureAwait(false);
        return attributes.TryGetValue("FilterPolicy", out var policy) && !string.IsNullOrWhiteSpace(policy)
            ? policy
            : null;
    }

    private async Task<Dictionary<string, string>> GetSubscriptionAttributesAsync(
        string subscriptionArn,
        CancellationToken cancellationToken)
    {
        var response = await CallAsync(
            ct => Sns.GetSubscriptionAttributesAsync(
                new GetSubscriptionAttributesRequest { SubscriptionArn = subscriptionArn },
                ct),
            cancellationToken).ConfigureAwait(false);
        return response.Attributes ?? new Dictionary<string, string>(StringComparer.Ordinal);
    }

    private async Task DeclareOnceAsync(
        string key,
        Func<CancellationToken, Task> declare,
        CancellationToken cancellationToken)
    {
        if (IsReady(key))
            return;

        await WaitAsync(_declareLock, cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsReady(key))
                return;

            using var timeout = LinkTimeout(cancellationToken);
            await declare(timeout.Token).ConfigureAwait(false);
            MarkReady(key);
        }
        finally
        {
            _declareLock.Release();
        }
    }

    private bool IsReady(string key)
    {
        lock (_ready)
            return _ready.Contains(key);
    }

    private void MarkReady(string key)
    {
        lock (_ready)
            _ready.Add(key);
    }

    private async Task<T> CallAsync<T>(Func<CancellationToken, Task<T>> call, CancellationToken cancellationToken)
    {
        using var timeout = LinkTimeout(cancellationToken);
        try
        {
            return await call(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"AWS SQS waited {_options.OperationTimeout} and timed out.");
        }
    }

    private async Task WaitAsync(SemaphoreSlim gate, CancellationToken cancellationToken)
    {
        if (await gate.WaitAsync(_options.OperationTimeout, cancellationToken).ConfigureAwait(false))
            return;

        throw new TimeoutException(
            $"AWS SQS waited {_options.OperationTimeout} for a local lock.");
    }

    private CancellationTokenSource LinkTimeout(CancellationToken cancellationToken)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(_options.OperationTimeout);
        return source;
    }

    private IAmazonSQS Sqs => GetClient(ref _sqs, _sqsFactory);

    private IAmazonSimpleNotificationService Sns => GetClient(ref _sns, _snsFactory);

    private TClient GetClient<TClient>(ref TClient? slot, Func<TClient> factory)
        where TClient : class
    {
        if (slot is not null)
            return slot;

        if (!_initLock.Wait(_options.OperationTimeout))
        {
            throw new TimeoutException(
                $"AWS SQS waited {_options.OperationTimeout} for a local lock.");
        }

        try
        {
            return slot ??= factory();
        }
        finally
        {
            _initLock.Release();
        }
    }

    private string? TopicArn(string topic)
    {
        lock (_topicArns)
            return _topicArns.TryGetValue(topic, out var arn) ? arn : null;
    }

    private void RememberTopic(string topic, string arn)
    {
        lock (_topicArns)
            _topicArns[topic] = arn;
    }

    private QueueHandle? FindQueue(string name)
    {
        lock (_queues)
            return _queues.TryGetValue(name, out var handle) ? handle : null;
    }

    private string? QueueUrl(string name) => FindQueue(name)?.Url;

    private string? SubscriptionArn(string key)
    {
        lock (_subscriptions)
            return _subscriptions.TryGetValue(key, out var arn) ? arn : null;
    }

    private void RememberSubscription(string key, string arn)
    {
        lock (_subscriptions)
            _subscriptions[key] = arn;
    }

    private static bool IsLiveQueue(Subscription? subscription)
        => subscription is not null &&
           string.Equals(subscription.Protocol, "sqs", StringComparison.OrdinalIgnoreCase) &&
           !string.IsNullOrWhiteSpace(subscription.SubscriptionArn) &&
           !string.Equals(subscription.SubscriptionArn, "PendingConfirmation", StringComparison.Ordinal);

    private static List<string> Normalize(IReadOnlyCollection<string> messageTypes)
        => messageTypes
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToList();

    private static int WholeSeconds(TimeSpan value, int min, int max)
    {
        var seconds = (int)Math.Ceiling(value.TotalSeconds);
        if (seconds < min)
            return min;
        if (seconds > max)
            return max;
        return seconds;
    }

    private static Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue> ToSnsAttributes(
        IReadOnlyDictionary<string, Amazon.SQS.Model.MessageAttributeValue> attributes)
    {
        var converted = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>(attributes.Count, StringComparer.Ordinal);
        foreach (var pair in attributes)
        {
            converted[pair.Key] = new Amazon.SimpleNotificationService.Model.MessageAttributeValue
            {
                DataType = pair.Value.DataType,
                StringValue = pair.Value.StringValue,
            };
        }

        return converted;
    }

    private static Dictionary<string, string> SubscribeAttributes(string? initialFilter)
    {
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["RawMessageDelivery"] = "true",
            ["FilterPolicyScope"] = "MessageAttributes",
        };
        if (!string.IsNullOrWhiteSpace(initialFilter))
            attributes["FilterPolicy"] = initialFilter;
        return attributes;
    }

    private static string AllowSns(string queueArn, string topicArn)
    {
        var policy = new Dictionary<string, object>
        {
            ["Version"] = "2012-10-17",
            ["Statement"] = new object[]
            {
                new Dictionary<string, object>
                {
                    ["Sid"] = "AllowSnsSend",
                    ["Effect"] = "Allow",
                    ["Principal"] = "*",
                    ["Action"] = "sqs:SendMessage",
                    ["Resource"] = queueArn,
                    ["Condition"] = new Dictionary<string, object>
                    {
                        ["ArnEquals"] = new Dictionary<string, string>
                        {
                            ["aws:SourceArn"] = topicArn,
                        },
                    },
                },
            },
        };
        return JsonSerializer.Serialize(policy);
    }

    private async Task SetRedriveAsync(QueueHandle queue, string deadLetterArn, CancellationToken cancellationToken)
    {
        var policy = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["deadLetterTargetArn"] = deadLetterArn,
            ["maxReceiveCount"] = _options.MaxReceiveCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
        });
        await CallAsync(
            ct => Sqs.SetQueueAttributesAsync(
                new SetQueueAttributesRequest
                {
                    QueueUrl = queue.Url,
                    Attributes = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["RedrivePolicy"] = policy,
                    },
                },
                ct),
            cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask DisposeClientAsync(object? client)
    {
        if (client is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        else if (client is IDisposable disposable)
            disposable.Dispose();
    }

    private static bool IsNotFound(Exception exception)
        => Matches(exception, HttpStatusCode.NotFound, "NotFound", "QueueDoesNotExist", "NonExistentQueue");

    private static bool IsAlreadyExists(Exception exception)
        => Matches(exception, HttpStatusCode.Conflict, "QueueAlreadyExists", "TopicAlreadyExists", "SubscriptionAlreadyExists");

    private static bool IsSubscriptionAttributeRace(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is not AmazonServiceException service)
                continue;

            var code = service.ErrorCode ?? string.Empty;
            if (!code.Contains("InvalidParameter", StringComparison.Ordinal))
                continue;

            var text = service.Message ?? string.Empty;
            if (text.Contains("already exists", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static bool Matches(Exception exception, HttpStatusCode status, params string[] codes)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is not AmazonServiceException service)
                continue;
            if (service.StatusCode == status)
                return true;
            if (service.ErrorCode is not null && codes.Contains(service.ErrorCode, StringComparer.Ordinal))
                return true;
        }

        return false;
    }

    private sealed record QueueHandle(string Url, string Arn);
}
