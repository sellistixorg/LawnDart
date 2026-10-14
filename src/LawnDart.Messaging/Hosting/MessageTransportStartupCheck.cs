using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Validates transport settings, declared message names, and optional topology setup
/// before the host finishes starting.
/// </summary>
public sealed class MessageTransportStartupCheck : IHostedService
{
    private readonly MessageTransportOptions _options;
    private readonly IReadOnlyList<MessageSubscription> _subscriptions;
    private readonly IReadOnlyList<IMessageTopologySetup> _setups;

    /// <summary>
    /// Creates the check from the registrations on the host.
    /// </summary>
    /// <param name="options">Common transport settings.</param>
    /// <param name="subscriptions">Message types recorded by reactor and processor registration.</param>
    /// <param name="setups">Topology setup implementations. Used only when setup is enabled.</param>
    public MessageTransportStartupCheck(
        IOptions<MessageTransportOptions> options,
        IEnumerable<MessageSubscription> subscriptions,
        IEnumerable<IMessageTopologySetup> setups)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(setups);

        _options = options.Value;
        _subscriptions = subscriptions.ToArray();
        _setups = setups.ToArray();
    }

    /// <summary>
    /// Checks settings and declared names, then runs topology setup when it is enabled.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _options.EnsureValid();

        foreach (var subscription in _subscriptions)
            MessageTypeNames.Require(subscription.MessageType);

        if (!_options.SetupTopology)
            return;

        if (_setups.Count == 0)
        {
            throw new InvalidOperationException(
                "MessageTransportOptions.SetupTopology is true, but no IMessageTopologySetup is registered.");
        }

        foreach (var setup in _setups)
            await setup.SetupAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>No work on shutdown.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
