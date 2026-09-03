using LawnDart;
using LawnDart.Demo.Academy.WebApi.Commands;
using LawnDart.Demo.Academy.WebApi.Domain.Events;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.Academy.WebApi.Handlers;

/// <summary>
/// Handles <see cref="RegisterStudentCommand"/> by appending a
/// <see cref="StudentRegistered"/> event to the student's stream.
/// </summary>
public sealed class RegisterStudentCommandHandler : ICommandHandler<RegisterStudentCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ITenantContextProvider _tenant;

    /// <summary>
    /// Initializes a new <see cref="RegisterStudentCommandHandler"/>.
    /// </summary>
    public RegisterStudentCommandHandler(
        IEventStore eventStore,
        ITenantContextProvider tenant)
    {
        _eventStore = eventStore;
        _tenant = tenant;
    }

    /// <inheritdoc/>
    public async Task HandleAsync(RegisterStudentCommand command, CancellationToken cancellationToken = default)
    {
        var tenantId = _tenant.GetTenantId() ?? "default";
        var streamId = $"{tenantId}:Student:{command.StudentId}";
        var studentEvents = await _eventStore.ReadStreamAsync(
            streamId,
            cancellationToken: cancellationToken);

        if (studentEvents.Any(e => e.Event is StudentRegistered))
            throw new DomainException($"Student {command.StudentId} is already registered.");

        var evt = new StudentRegistered(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            StudentId: command.StudentId,
            Name: command.Name,
            Email: command.Email);

        await _eventStore.AppendAsync(
            streamId,
            [evt],
            cancellationToken: cancellationToken);
    }
}

/// <summary>
/// Handles <see cref="EnrollStudentCommand"/> by appending a
/// <see cref="StudentEnrolled"/> event and a <see cref="SeatReserved"/> event.
/// </summary>
public sealed class EnrollStudentCommandHandler : ICommandHandler<EnrollStudentCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ITenantContextProvider _tenant;

    /// <summary>
    /// Initializes a new <see cref="EnrollStudentCommandHandler"/>.
    /// </summary>
    public EnrollStudentCommandHandler(
        IEventStore eventStore,
        ITenantContextProvider tenant)
    {
        _eventStore = eventStore;
        _tenant = tenant;
    }

    /// <inheritdoc/>
    public async Task HandleAsync(EnrollStudentCommand command, CancellationToken cancellationToken = default)
    {
        var tenantId = _tenant.GetTenantId() ?? "default";
        var studentStreamId = $"{tenantId}:Student:{command.StudentId}";
        var sectionStreamId = $"{tenantId}:Section:{command.SectionId}";
        var studentEvents = await _eventStore.ReadStreamAsync(
            studentStreamId,
            cancellationToken: cancellationToken);
        var sectionEvents = await _eventStore.ReadStreamAsync(
            sectionStreamId,
            cancellationToken: cancellationToken);

        if (!studentEvents.Any(e => e.Event is StudentRegistered))
            throw new DomainException("Student must be registered before enrolling.");

        var sectionCreated = sectionEvents
            .Select(e => e.Event)
            .OfType<SectionCreated>()
            .FirstOrDefault();
        if (sectionCreated is null)
            throw new DomainException("Section does not exist.");

        if (AcademyEnrollmentRules.HasActiveEnrollment(studentEvents, command.SectionId))
            throw new DomainException(
                $"Student {command.StudentId} is already enrolled in section {command.SectionId}.");

        var activeSeatReservations = sectionEvents.Count(e => e.Event is SeatReserved)
            - sectionEvents.Count(e => e.Event is SeatReleased);
        if (activeSeatReservations >= sectionCreated.TotalSeats)
            throw new DomainException("No seats are currently available.");

        await _eventStore.AppendAsync(
            studentStreamId,
            [new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, command.StudentId, command.SectionId, sectionCreated.Title)],
            cancellationToken: cancellationToken);

        await _eventStore.AppendAsync(
            sectionStreamId,
            [new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, command.SectionId, command.StudentId)],
            cancellationToken: cancellationToken);
    }
}

/// <summary>
/// Handles <see cref="CancelEnrollmentCommand"/> by appending an
/// <see cref="EnrollmentCancelled"/> event and a <see cref="SeatReleased"/> event.
/// </summary>
public sealed class CancelEnrollmentCommandHandler : ICommandHandler<CancelEnrollmentCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ITenantContextProvider _tenant;

    /// <summary>
    /// Initializes a new <see cref="CancelEnrollmentCommandHandler"/>.
    /// </summary>
    public CancelEnrollmentCommandHandler(
        IEventStore eventStore,
        ITenantContextProvider tenant)
    {
        _eventStore = eventStore;
        _tenant = tenant;
    }

    /// <inheritdoc/>
    public async Task HandleAsync(CancelEnrollmentCommand command, CancellationToken cancellationToken = default)
    {
        var tenantId = _tenant.GetTenantId() ?? "default";
        var studentStreamId = $"{tenantId}:Student:{command.StudentId}";
        var studentEvents = await _eventStore.ReadStreamAsync(
            studentStreamId,
            cancellationToken: cancellationToken);

        if (!AcademyEnrollmentRules.HasActiveEnrollment(studentEvents, command.SectionId))
            throw new DomainException("No active enrollment exists for this section.");

        await _eventStore.AppendAsync(
            studentStreamId,
            [new EnrollmentCancelled(Guid.NewGuid(), DateTime.UtcNow, command.StudentId, command.SectionId, command.Reason)],
            cancellationToken: cancellationToken);

        await _eventStore.AppendAsync(
            $"{tenantId}:Section:{command.SectionId}",
            [new SeatReleased(Guid.NewGuid(), DateTime.UtcNow, command.SectionId, command.StudentId)],
            cancellationToken: cancellationToken);
    }
}

/// <summary>
/// Handles <see cref="CreateCourseSectionCommand"/> by appending a
/// <see cref="SectionCreated"/> event to the section's stream.
/// </summary>
public sealed class CreateCourseSectionCommandHandler : ICommandHandler<CreateCourseSectionCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ITenantContextProvider _tenant;

    /// <summary>
    /// Initializes a new <see cref="CreateCourseSectionCommandHandler"/>.
    /// </summary>
    public CreateCourseSectionCommandHandler(
        IEventStore eventStore,
        ITenantContextProvider tenant)
    {
        _eventStore = eventStore;
        _tenant = tenant;
    }

    /// <inheritdoc/>
    public async Task HandleAsync(CreateCourseSectionCommand command, CancellationToken cancellationToken = default)
    {
        var tenantId = _tenant.GetTenantId() ?? "default";
        var streamId = $"{tenantId}:Section:{command.SectionId}";
        var sectionEvents = await _eventStore.ReadStreamAsync(
            streamId,
            cancellationToken: cancellationToken);

        if (sectionEvents.Any(e => e.Event is SectionCreated))
            throw new DomainException($"Section {command.SectionId} already exists.");
        if (command.TotalSeats <= 0)
            throw new DomainException("TotalSeats must be greater than zero.");

        var evt = new SectionCreated(
            Id: Guid.NewGuid(),
            Timestamp: DateTime.UtcNow,
            SectionId: command.SectionId,
            Title: command.Title,
            TotalSeats: command.TotalSeats);

        await _eventStore.AppendAsync(
            streamId,
            [evt],
            cancellationToken: cancellationToken);
    }
}

internal static class AcademyEnrollmentRules
{
    internal static bool HasActiveEnrollment(
        IReadOnlyList<SequencedEvent> studentEvents,
        Guid sectionId)
    {
        var active = false;

        foreach (var sequencedEvent in studentEvents.OrderBy(e => e.Version))
        {
            switch (sequencedEvent.Event)
            {
                case StudentEnrolled enrolled when enrolled.SectionId == sectionId:
                    active = true;
                    break;

                case EnrollmentCancelled cancelled when cancelled.SectionId == sectionId:
                    active = false;
                    break;
            }
        }

        return active;
    }
}
