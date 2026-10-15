namespace LawnDart.Messaging;

/// <summary>
/// Maps a declared message type name to the topic or exchange that carries it.
/// Broker transports call <see cref="TopicFor"/> when they publish or subscribe.
/// The default returns the declared name, so each type has its own topic.
/// </summary>
public interface ITopicMapper
{
    /// <summary>
    /// Returns the topic or exchange name for <paramref name="messageType"/>.
    /// </summary>
    /// <param name="messageType">Declared name from <see cref="MessageTypeNames.Require"/>.</param>
    string TopicFor(string messageType);
}

/// <summary>
/// Uses the declared message type name as the topic.
/// One topic per type. This is the default.
/// </summary>
public sealed class DeclaredTypeTopicMapper : ITopicMapper
{
    /// <summary>The shared instance.</summary>
    public static DeclaredTypeTopicMapper Instance { get; } = new();

    /// <summary>Creates a mapper that returns the declared name.</summary>
    public DeclaredTypeTopicMapper()
    {
    }

    /// <inheritdoc />
    public string TopicFor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        return messageType;
    }
}

/// <summary>
/// Sends every message type to one topic or exchange.
/// </summary>
public sealed class SharedTopicMapper : ITopicMapper
{
    private readonly string _topic;

    /// <summary>
    /// Creates a mapper that returns <paramref name="topic"/> for every message type.
    /// </summary>
    /// <param name="topic">Topic or exchange name.</param>
    public SharedTopicMapper(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _topic = topic;
    }

    /// <inheritdoc />
    public string TopicFor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        return _topic;
    }
}

/// <summary>
/// Maps listed message types to topics. A type that is not listed uses the declared name,
/// or <see cref="FallbackTopic"/> when that is set.
/// Use this to group types that share a bounded context.
/// </summary>
public sealed class GroupTopicMapper : ITopicMapper
{
    private readonly Dictionary<string, string> _topics;
    private readonly string? _fallbackTopic;

    /// <summary>
    /// Creates a mapper from declared message type names to topic names.
    /// </summary>
    /// <param name="topics">Declared name to topic. Comparison is ordinal.</param>
    /// <param name="fallbackTopic">
    /// Topic for a type that is not listed.
    /// When this is <c>null</c>, an unlisted type uses its declared name.
    /// </param>
    public GroupTopicMapper(IReadOnlyDictionary<string, string> topics, string? fallbackTopic = null)
    {
        ArgumentNullException.ThrowIfNull(topics);
        if (fallbackTopic is not null && string.IsNullOrWhiteSpace(fallbackTopic))
        {
            throw new ArgumentException(
                "Fallback topic is required when it is set.",
                nameof(fallbackTopic));
        }

        _topics = new Dictionary<string, string>(topics.Count, StringComparer.Ordinal);
        foreach (var pair in topics)
        {
            if (string.IsNullOrWhiteSpace(pair.Key))
            {
                throw new ArgumentException(
                    "A message type name is required.",
                    nameof(topics));
            }

            if (string.IsNullOrWhiteSpace(pair.Value))
            {
                throw new ArgumentException(
                    $"Topic for '{pair.Key}' is required.",
                    nameof(topics));
            }

            _topics[pair.Key] = pair.Value;
        }

        _fallbackTopic = fallbackTopic;
    }

    /// <summary>Topic used for a type that is not listed. <c>null</c> means the declared name.</summary>
    public string? FallbackTopic => _fallbackTopic;

    /// <inheritdoc />
    public string TopicFor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        if (_topics.TryGetValue(messageType, out var topic))
            return topic;

        return _fallbackTopic ?? messageType;
    }
}

/// <summary>
/// Uses the text before the first separator as the topic.
/// <c>orders.placed</c> with separator <c>.</c> maps to <c>orders</c>.
/// A name with no separator maps to itself.
/// </summary>
public sealed class PrefixTopicMapper : ITopicMapper
{
    private readonly char _separator;

    /// <summary>
    /// Creates a mapper that groups by the prefix before <paramref name="separator"/>.
    /// </summary>
    /// <param name="separator">Character that ends the topic prefix. Defaults to <c>.</c>.</param>
    public PrefixTopicMapper(char separator = '.')
    {
        if (char.IsWhiteSpace(separator))
        {
            throw new ArgumentException(
                "Separator must be a visible character.",
                nameof(separator));
        }

        _separator = separator;
    }

    /// <summary>Character that ends the topic prefix.</summary>
    public char Separator => _separator;

    /// <inheritdoc />
    public string TopicFor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        var index = messageType.IndexOf(_separator);
        if (index <= 0)
            return messageType;

        return messageType[..index];
    }
}

/// <summary>
/// Replaces each period in the inner mapper's topic with a hyphen.
/// The default mapper is unchanged. Use this when a broker rejects periods,
/// such as Amazon SNS. <c>order.placed</c> becomes <c>order-placed</c>.
/// </summary>
public sealed class SanitizingTopicMapper : ITopicMapper
{
    private readonly ITopicMapper _inner;

    /// <summary>
    /// Creates a mapper that sanitizes topics from <paramref name="inner"/>.
    /// When <paramref name="inner"/> is omitted, the declared type name is sanitized.
    /// </summary>
    /// <param name="inner">Mapper that runs first. Null uses <see cref="DeclaredTypeTopicMapper.Instance"/>.</param>
    public SanitizingTopicMapper(ITopicMapper? inner = null)
    {
        _inner = inner ?? DeclaredTypeTopicMapper.Instance;
    }

    /// <summary>The mapper that runs before periods are replaced.</summary>
    public ITopicMapper Inner => _inner;

    /// <inheritdoc />
    public string TopicFor(string messageType)
    {
        var topic = _inner.TopicFor(messageType);
        return topic.Replace('.', '-');
    }

    /// <summary>
    /// Throws when two declared names sanitize to the same topic and the inner mapper
    /// did not already group them. <c>order.placed</c> and <c>order-placed</c> collide.
    /// Two names that the inner mapper sends to one topic do not.
    /// Host startup calls this for the recorded message types.
    /// </summary>
    /// <param name="messageTypes">Declared message type names.</param>
    public void EnsureNoCollisions(IEnumerable<string> messageTypes)
    {
        ArgumentNullException.ThrowIfNull(messageTypes);
        var seen = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var name in messageTypes)
        {
            if (string.IsNullOrWhiteSpace(name))
                continue;

            var topic = TopicFor(name);
            if (!seen.TryGetValue(topic, out var other))
            {
                seen[topic] = name;
                continue;
            }

            if (string.Equals(other, name, StringComparison.Ordinal))
                continue;

            var innerOther = _inner.TopicFor(other);
            var innerName = _inner.TopicFor(name);
            if (string.Equals(innerOther, innerName, StringComparison.Ordinal))
                continue;

            throw new InvalidOperationException(
                $"'{name}' and '{other}' sanitize to the same topic '{topic}'. " +
                "Use distinct names, or map them to different topics before sanitizing.");
        }
    }
}

/// <summary>
/// Maps a message type with a delegate.
/// </summary>
public sealed class DelegateTopicMapper : ITopicMapper
{
    private readonly Func<string, string> _map;

    /// <summary>
    /// Creates a mapper that calls <paramref name="map"/>.
    /// </summary>
    /// <param name="map">Returns the topic for a declared message type name.</param>
    public DelegateTopicMapper(Func<string, string> map)
    {
        ArgumentNullException.ThrowIfNull(map);
        _map = map;
    }

    /// <inheritdoc />
    public string TopicFor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        var topic = _map(messageType);
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new InvalidOperationException(
                $"Topic mapper returned no topic for '{messageType}'.");
        }

        return topic;
    }
}

/// <summary>
/// Chooses the topic mapper a transport should use.
/// </summary>
public static class TopicMapperSelection
{
    /// <summary>
    /// A mapper set on the options wins when it is not <see cref="DeclaredTypeTopicMapper.Instance"/>.
    /// Otherwise the mapper registered in DI is used.
    /// When neither is set, the declared type name is the topic.
    /// </summary>
    /// <param name="configured">Mapper from <see cref="MessageTransportOptions.TopicMapper"/>.</param>
    /// <param name="registered">Mapper registered as <see cref="ITopicMapper"/>, or <c>null</c>.</param>
    public static ITopicMapper Select(ITopicMapper? configured, ITopicMapper? registered)
    {
        if (configured is not null && !ReferenceEquals(configured, DeclaredTypeTopicMapper.Instance))
            return configured;

        return registered ?? DeclaredTypeTopicMapper.Instance;
    }

    /// <summary>
    /// Returns the mapper on <paramref name="options"/>.
    /// A null value uses the declared type name.
    /// Startup copies a registered <see cref="ITopicMapper"/> onto the options when they still have the default instance.
    /// </summary>
    /// <param name="options">Transport options.</param>
    public static ITopicMapper FromOptions(MessageTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.TopicMapper ?? DeclaredTypeTopicMapper.Instance;
    }
}
