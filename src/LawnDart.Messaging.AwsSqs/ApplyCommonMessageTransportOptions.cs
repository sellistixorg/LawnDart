using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Copies <see cref="MessageTransportOptions"/> configuration onto <see cref="AwsSqsTransportOptions"/>.
/// <c>Configure&lt;MessageTransportOptions&gt;</c> still applies when the host registers the SQS transport.
/// </summary>
internal sealed class ApplyCommonMessageTransportOptions :
    IConfigureOptions<AwsSqsTransportOptions>,
    IPostConfigureOptions<AwsSqsTransportOptions>
{
    private readonly IEnumerable<IConfigureOptions<MessageTransportOptions>> _configure;
    private readonly IEnumerable<IPostConfigureOptions<MessageTransportOptions>> _postConfigure;

    public ApplyCommonMessageTransportOptions(
        IEnumerable<IConfigureOptions<MessageTransportOptions>> configure,
        IEnumerable<IPostConfigureOptions<MessageTransportOptions>> postConfigure)
    {
        _configure = configure;
        _postConfigure = postConfigure;
    }

    public void Configure(AwsSqsTransportOptions options)
    {
        foreach (var item in _configure)
            item.Configure(options);
    }

    public void PostConfigure(string? name, AwsSqsTransportOptions options)
    {
        foreach (var item in _postConfigure)
            item.PostConfigure(name, options);
    }
}

/// <summary>
/// Copies a registered <see cref="ITopicMapper"/> onto <see cref="AwsSqsTransportOptions"/>
/// when the options still use the default mapper.
/// </summary>
internal sealed class ApplyRegisteredTopicMapper : IPostConfigureOptions<AwsSqsTransportOptions>
{
    private readonly ITopicMapper? _registered;

    public ApplyRegisteredTopicMapper(ITopicMapper? registered) => _registered = registered;

    public void PostConfigure(string? name, AwsSqsTransportOptions options)
    {
        options.TopicMapper = TopicMapperSelection.Select(options.TopicMapper, _registered);
    }
}
