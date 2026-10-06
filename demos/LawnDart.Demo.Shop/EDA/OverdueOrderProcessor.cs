using System.Text.Json;
using LawnDart;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Projections;
using LawnDart.Patterns.TaskProcessing;
using LawnDart.Projections.Storage;

namespace LawnDart.Demo.Shop.EDA;

/// <summary>
/// Periodically scans for orders stuck in Pending state beyond the configured threshold
/// and cancels them. Demonstrates the "scheduled task emits commands" pattern.
/// </summary>
public sealed class OverdueOrderProcessor : ITaskProcessor
{
    private readonly IViewStore _viewStore;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _threshold;

    public OverdueOrderProcessor(
        IViewStore viewStore,
        TimeProvider? timeProvider = null,
        TimeSpan? threshold = null)
    {
        _viewStore    = viewStore;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _threshold    = threshold ?? TimeSpan.FromSeconds(30);
    }

    public async Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;

        // Read from the AllOrders global projection view
        // Storage key is "{logicalName}:v{version}". Single-node global views use instance "global".
        var viewJson = await _viewStore.GetViewAsync("AllOrders:v1", "global", cancellationToken)
            .ConfigureAwait(false);
        if (string.IsNullOrEmpty(viewJson))
            return [];

        var orders = JsonSerializer.Deserialize<List<OrderSummaryView>>(viewJson,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (orders is null)
            return [];

        return orders
            .Where(o => o.Status == "Pending" && now - o.PlacedAt > _threshold)
            .Select(o => (ICommand)new CancelOrderCommand(
                Guid.NewGuid(),
                o.OrderId,
                $"Order expired after {_threshold.TotalSeconds:F0}s without payment.",
                o.TenantId))
            .ToList();
    }
}
