using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.Demo.Shop.Infrastructure;
using LawnDart.EventSourcing.Context;
using LawnDart.Messaging;

namespace LawnDart.Demo.Shop.EDA;

/// <summary>
/// Logs reactor and task commands, then dispatches them through
/// <see cref="ContextAwareCommandDispatcher"/> so keyed handlers and ambient
/// message context stay intact.
/// </summary>
public sealed class ShopCommandDispatcher : ICommandDispatcher
{
    private readonly ContextAwareCommandDispatcher _inner;
    private readonly CommandEventLog _log;

    /// <summary>
    /// Creates a dispatcher that records commands in <paramref name="log"/> and then routes them.
    /// </summary>
    public ShopCommandDispatcher(
        ICommandContextRegistry registry,
        IServiceProvider services,
        CommandEventLog log,
        ILogger<ContextAwareCommandDispatcher>? logger = null)
    {
        _inner = new ContextAwareCommandDispatcher(registry, services, logger);
        _log = log;
    }

    /// <inheritdoc />
    public async Task DispatchAsync(
        ICommand command,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        var reactorName = context.Headers.TryGetValue("reactor", out var name) ? name : "Reactor";
        var correlationId = context.CorrelationId ?? Guid.NewGuid().ToString();
        var userId = context.UserId ?? "system";

        _log.RecordCommand(
            commandId: command.Id,
            commandType: command.GetType().Name,
            correlationId: correlationId,
            userId: userId,
            userName: $"SYSTEM via {reactorName}",
            isReactorIssued: true,
            reactorName: reactorName);

        using var scope = CorrelationScope.Begin(new CorrelationContext(
            correlationId, userId, $"SYSTEM via {reactorName}", reactorName));

        var headers = new Dictionary<string, string>(context.Headers, StringComparer.Ordinal)
        {
            ["shop-dispatch"] = "background"
        };
        var outbound = new MessageContext
        {
            MessageId = context.MessageId,
            CorrelationId = context.CorrelationId,
            CausationId = context.CausationId,
            TenantId = context.TenantId,
            UserId = context.UserId,
            EnqueuedAt = context.EnqueuedAt,
            TransportType = context.TransportType,
            Headers = headers
        };

        await _inner.DispatchAsync(command, outbound, cancellationToken).ConfigureAwait(false);
    }
}
