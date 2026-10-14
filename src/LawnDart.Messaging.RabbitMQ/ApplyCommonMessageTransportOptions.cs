using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Copies <see cref="MessageTransportOptions"/> configuration onto <see cref="RabbitMqTransportOptions"/>.
/// <c>Configure&lt;MessageTransportOptions&gt;</c> still applies when the host registers the RabbitMQ transport.
/// </summary>
internal sealed class ApplyCommonMessageTransportOptions :
    IConfigureOptions<RabbitMqTransportOptions>,
    IPostConfigureOptions<RabbitMqTransportOptions>
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

    public void Configure(RabbitMqTransportOptions options)
    {
        foreach (var item in _configure)
            item.Configure(options);
    }

    public void PostConfigure(string? name, RabbitMqTransportOptions options)
    {
        foreach (var item in _postConfigure)
            item.PostConfigure(name, options);
    }
}
