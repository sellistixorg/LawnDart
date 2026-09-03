using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Demo.Academy.Domain.Student;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.Course;
using LawnDart.Demo.Academy.Domain.Course.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;
using LawnDart.Demo.Academy.EDA;
using LawnDart.Demo.Academy.Projections;

namespace LawnDart.Demo.Academy.Showcases;

/// <summary>
/// Showcase A: Traditional Aggregate Root + EDA Choreography.
///
/// Workflow:
///   Step 1: RegisterStudent          → StudentRegistered
///   Step 2: CreateCourse             → CourseCreated
///   Step 3: CreateSection (5 seats)  → SectionCreated
///   Step 4: ReserveSeat              → SeatReserved        [optimistic lock on section stream]
///   Step 5: SeatReservationConfirmed → [PaymentReactor]    → ProcessPaymentCommand
///   Step 6: ProcessPayment           → StudentPaymentProcessed
///   Step 7: PaymentAuthorised        → [ConfirmationReactor] → SendConfirmationCommand
///   Step 8: EnrollStudent            → StudentEnrolled
///   Step 9: SendConfirmation         → RegistrationConfirmationSent
///
/// Total round-trips: 5 (one per aggregate write + two broker hops)
/// Consistency: Eventual (seat reservation and payment are separate transactions)
/// </summary>
public class ShowcaseA_TraditionalEda
{
    private readonly IAggregateRepository _repository;
    private readonly CourseSectionProjector _sectionProjector;
    private readonly StudentTranscriptProjector _transcriptProjector;

    public ShowcaseA_TraditionalEda(
        IAggregateRepository repository,
        CourseSectionProjector sectionProjector,
        StudentTranscriptProjector transcriptProjector)
    {
        _repository = repository;
        _sectionProjector = sectionProjector;
        _transcriptProjector = transcriptProjector;
    }

    public async Task<ShowcaseAResult> RunAsync(Guid studentId, Guid courseId, Guid sectionId, string studentName)
    {
        var result = new ShowcaseAResult();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // ── Step 1: Register student ──────────────────────────────────────────
        var student = await _repository.GetOrCreateAsync<Student>(studentId);
        await _repository.HandleCommandAsync(student,
            new RegisterStudentCommand(Guid.NewGuid(), studentId, studentName, $"{studentName.ToLowerInvariant().Replace(" ", ".")}@academy.example"));
        _transcriptProjector.Apply(new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, studentId, studentName,
            student.State.Email));
        result.Steps.Add($"[Step 1] StudentRegistered: {student.State.Name}");

        // ── Step 2: Create course ─────────────────────────────────────────────
        var course = await _repository.GetOrCreateAsync<Course>(courseId);
        await _repository.HandleCommandAsync(course,
            new CreateCourseCommand(Guid.NewGuid(), courseId, "Advanced Event Sourcing", "Master ES, CQRS, and DCB", 299m));
        result.Steps.Add($"[Step 2] CourseCreated: {course.State.Title} @ £{course.State.Price:F2}");

        // ── Step 3: Create section with 5 seats ──────────────────────────────
        var section = await _repository.GetOrCreateAsync<CourseSection>(sectionId);
        await _repository.HandleCommandAsync(section,
            new CreateSectionCommand(Guid.NewGuid(), sectionId, courseId, "Morning Cohort", 5));
        _sectionProjector.Apply(new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Morning Cohort", 5));
        result.Steps.Add($"[Step 3] SectionCreated: {section.State.Title} ({section.State.TotalSeats} seats)");

        // ── Step 4: Reserve a seat (optimistic concurrency on section stream) ─
        await _repository.HandleCommandAsync(section,
            new ReserveSeatCommand(Guid.NewGuid(), sectionId, studentId));
        _sectionProjector.Apply(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, sectionId, studentId));
        result.Steps.Add($"[Step 4] SeatReserved → seats remaining: {section.State.SeatsAvailable}");

        // ── Step 5-7: EDA Choreography via in-memory transport ────────────────
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions()));

        var paymentReactor = new PaymentReactor();
        var confirmationReactor = new ConfirmationReactor();
        var completedSteps = new List<string>();

        _ = transport.SubscribeAsync<SeatReservationConfirmed>(async (e, ctx, ct) =>
        {
            if (ctx.MessageId is not null && await inbox.IsProcessedAsync(ctx.MessageId, ct))
                return;

            var commands = (await paymentReactor.ReactAsync(e, ctx, ct)).ToList();

            if (ctx.MessageId is not null)
                await inbox.MarkProcessedAsync(ctx.MessageId, DateTimeOffset.UtcNow, ct);

            foreach (var cmd in commands.OfType<ProcessPaymentCommand>())
            {
                completedSteps.Add($"[Step 5] PaymentReactor: SeatReservationConfirmed → ProcessPaymentCommand");

                // Simulate payment processing on student aggregate
                var s = await _repository.GetOrCreateAsync<Student>(cmd.StudentId);
                await _repository.HandleCommandAsync(s,
                    new ProcessPaymentCommand(Guid.NewGuid(), cmd.StudentId, cmd.CourseId, cmd.Amount));
                completedSteps.Add($"[Step 6] StudentPaymentProcessed: £{cmd.Amount:F2} for CourseId={cmd.CourseId}");
                _transcriptProjector.Apply(new StudentPaymentProcessed(Guid.NewGuid(), DateTime.UtcNow,
                    cmd.StudentId, cmd.CourseId, cmd.Amount, "REF-DEMO"));

                // Publish PaymentAuthorised
                await transport.PublishAsync(
                    new PaymentAuthorised(Guid.NewGuid(), DateTime.UtcNow, cmd.StudentId, cmd.CourseId, "PAY-REF"),
                    ctx.CreateChild());
            }
        });

        _ = transport.SubscribeAsync<PaymentAuthorised>(async (e, ctx, ct) =>
        {
            var commands = (await confirmationReactor.ReactAsync(e, ctx, ct)).ToList();

            foreach (var cmd in commands.OfType<SendConfirmationCommand>())
            {
                completedSteps.Add($"[Step 7] ConfirmationReactor: PaymentAuthorised → SendConfirmationCommand");

                // Enroll student
                var s = await _repository.GetOrCreateAsync<Student>(e.StudentId);
                await _repository.HandleCommandAsync(s,
                    new EnrollStudentCommand(Guid.NewGuid(), e.StudentId, e.CourseId));
                _transcriptProjector.Apply(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, e.StudentId, e.CourseId));
                completedSteps.Add($"[Step 8] StudentEnrolled: StudentId={e.StudentId}");

                // Send confirmation
                await _repository.HandleCommandAsync(s,
                    new SendConfirmationCommand(Guid.NewGuid(), e.StudentId, e.CourseId, s.State.Email));
                _transcriptProjector.Apply(new RegistrationConfirmationSent(Guid.NewGuid(), DateTime.UtcNow,
                    e.StudentId, e.CourseId, s.State.Email));
                completedSteps.Add($"[Step 9] RegistrationConfirmationSent to {s.State.Email}");
            }
        });

        // Trigger the chain
        await transport.PublishAsync(
            new SeatReservationConfirmed(Guid.NewGuid(), DateTime.UtcNow, studentId, courseId, sectionId, course.State.Price),
            new MessageContext { MessageId = Guid.NewGuid().ToString(), CorrelationId = Guid.NewGuid().ToString() });

        result.Steps.AddRange(completedSteps);
        result.TotalRoundTrips = 5;
        result.ConsistencyModel = "Eventual (seat reservation and payment are separate transactions)";
        result.ElapsedMs = (int)sw.ElapsedMilliseconds;
        result.SeatsRemaining = section.State.SeatsAvailable;
        result.Completed = completedSteps.Any(s => s.Contains("RegistrationConfirmationSent"));
        return result;
    }
}

public class ShowcaseAResult
{
    public List<string> Steps { get; set; } = [];
    public int TotalRoundTrips { get; set; }
    public string ConsistencyModel { get; set; } = string.Empty;
    public int ElapsedMs { get; set; }
    public int SeatsRemaining { get; set; }
    public bool Completed { get; set; }
}
