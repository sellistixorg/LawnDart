using LawnDart;
using LawnDart.Patterns.TaskProcessing;
using LawnDart.Demo.Academy.Domain.Student.Commands;

namespace LawnDart.Demo.Academy.EDA;

/// <summary>
/// Represents a pending registration that may be overdue.
/// In production this would be sourced from a read model / projection.
/// </summary>
public record PendingRegistration(
    Guid StudentId,
    Guid CourseId,
    DateTime RegisteredAt,
    TimeSpan MaxPendingDuration);

/// <summary>
/// Task Processor: periodically scans for registrations that have been
/// pending (no payment received) beyond their allowed window and emits
/// cancellation commands. Demonstrates the State → Command pattern.
/// </summary>
public sealed class OverdueRegistrationProcessor : ITaskProcessor
{
    private readonly IEnumerable<PendingRegistration> _pendingRegistrations;
    private readonly TimeProvider _timeProvider;

    public OverdueRegistrationProcessor(
        IEnumerable<PendingRegistration> pendingRegistrations,
        TimeProvider? timeProvider = null)
    {
        _pendingRegistrations = pendingRegistrations;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<IEnumerable<ICommand>> ProcessTasksAsync(CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;

        var commands = _pendingRegistrations
            .Where(r => now - r.RegisteredAt > r.MaxPendingDuration)
            .Select(r => (ICommand)new CancelRegistrationCommand(
                Guid.NewGuid(),
                r.StudentId,
                r.CourseId,
                Reason: $"Registration expired after {r.MaxPendingDuration.TotalHours:F0}h without payment."))
            .ToList();

        return Task.FromResult<IEnumerable<ICommand>>(commands);
    }
}
