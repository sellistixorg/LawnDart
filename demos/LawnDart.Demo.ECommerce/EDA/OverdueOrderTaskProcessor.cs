using LawnDart.Messaging;
using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Demo.ECommerce.EDA;

/// <summary>
/// Task processor: polls for overdue orders -> <see cref="CancelOverdueOrderCommand"/> per order.
/// <para>
/// Runs on a configurable interval (see <see cref="TaskProcessorOptions.PollingInterval"/>).
/// In a real system, this would query the order store for orders past their payment deadline.
/// </para>
/// </summary>
public class OverdueOrderTaskProcessor : ITaskProcessor
{
    private readonly IReadOnlyList<string> _overdueOrderIds;

    /// <param name="overdueOrderIds">
    /// Simulated list of overdue order IDs.
    /// In production, inject an <c>IOrderRepository</c> and query for overdue orders.
    /// </param>
    public OverdueOrderTaskProcessor(IEnumerable<string> overdueOrderIds)
    {
        _overdueOrderIds = [.. overdueOrderIds];
    }

    public Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
    {
        var commands = _overdueOrderIds.Select(id =>
            (ICommand)new CancelOverdueOrderCommand(
                Id: Guid.NewGuid(),
                OrderId: id,
                Reason: "Payment overdue by more than 30 days"));

        return Task.FromResult(commands);
    }
}
