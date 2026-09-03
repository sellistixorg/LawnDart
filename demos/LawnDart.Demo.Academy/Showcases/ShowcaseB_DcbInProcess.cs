using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.Demo.Academy.Domain.Student;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.Course;
using LawnDart.Demo.Academy.Domain.Course.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;
using LawnDart.Demo.Academy.Enrollment;
using LawnDart.Demo.Academy.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.Academy.Showcases;

/// <summary>
/// Showcase B: DCB In-Process (same workflow, no broker).
///
/// Workflow:
///   Step 1: RegisterStudent          → StudentRegistered
///   Step 2: CreateCourse             → CourseCreated
///   Step 3: CreateSection (5 seats)  → SectionCreated
///   Step 4: EnrollmentEntity.HandleCommandAsync(EnrollStudentCommand)
///             → reads: student:{id} + section:{id}  (1 read trip)
///             → atomic append: SeatReserved + StudentEnrolled  (1 write trip)
///             → AppendCondition: fails if any seat was taken since we read
///   Step 5: EnrollmentEntity.HandleCommandAsync(SendConfirmationCommand)
///             → RegistrationConfirmationSent
///
/// Total round-trips: 3 (setup) + 2 (DCB load + DCB append)
/// Consistency: Strong — seat reservation and enrollment are atomic.
/// No broker needed — no inbox, no idempotency key management.
///
/// Key DCB insight: the EnrollmentEntity loads a consistent snapshot of ALL
/// events tagged student:{id} + section:{id}.  This means bootstrap events
/// (StudentRegistered, SectionCreated) MUST carry BOTH tags so the entity
/// state is populated correctly before the enrollment guard runs.
/// </summary>
public class ShowcaseB_DcbInProcess
{
    private readonly IAggregateRepository _aggregateRepository;
    private readonly IDcbRepository _dcbRepository;
    private readonly IEventStore _eventStore;
    private readonly CourseSectionProjector _sectionProjector;
    private readonly StudentTranscriptProjector _transcriptProjector;

    public ShowcaseB_DcbInProcess(
        IAggregateRepository aggregateRepository,
        IDcbRepository dcbRepository,
        IEventStore eventStore,
        CourseSectionProjector sectionProjector,
        StudentTranscriptProjector transcriptProjector)
    {
        _aggregateRepository = aggregateRepository;
        _dcbRepository = dcbRepository;
        _eventStore = eventStore;
        _sectionProjector = sectionProjector;
        _transcriptProjector = transcriptProjector;
    }

    public async Task<ShowcaseBResult> RunAsync(Guid studentId, Guid courseId, Guid sectionId, string studentName)
    {
        var result = new ShowcaseBResult();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // ── Step 1: Register student (traditional aggregate) ──────────────────
        var student = await _aggregateRepository.GetOrCreateAsync<Student>(studentId);
        await _aggregateRepository.HandleCommandAsync(student,
            new RegisterStudentCommand(Guid.NewGuid(), studentId, studentName,
                $"{studentName.ToLowerInvariant().Replace(" ", ".")}@academy.example"));

        var studentEmail = student.State.Email;
        _transcriptProjector.Apply(new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, studentId, studentName, studentEmail));
        result.Steps.Add($"[Step 1] StudentRegistered: {student.State.Name}");

        // ── Step 2: Create course (traditional aggregate) ─────────────────────
        var course = await _aggregateRepository.GetOrCreateAsync<Course>(courseId);
        await _aggregateRepository.HandleCommandAsync(course,
            new CreateCourseCommand(Guid.NewGuid(), courseId, "Advanced Event Sourcing", "Master ES, CQRS, and DCB", 299m));
        result.Steps.Add($"[Step 2] CourseCreated: {course.State.Title} @ £{course.State.Price:F2}");

        // ── Step 3 & seed: Write bootstrap events with BOTH tags ──────────────
        // The DCB entity queries by [student:{id}, section:{id}].
        // QueryItem.ByTags returns events that carry ALL of the given tags.
        // Bootstrap events (SectionCreated, StudentRegistered) must therefore carry
        // both tags so they appear in the entity's state when it is loaded.
        var enrollmentTags = EnrollmentEntity.GetTags(studentId, sectionId);
        var meta = new LawnDart.Metadata.EventMetadata { Timestamp = DateTime.UtcNow };

        await _eventStore.AppendAsync(
            $"dcb-bootstrap:{sectionId}",
            [new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Morning Cohort", 5)],
            expectedVersion: null,
            metadata: meta,
            tags: enrollmentTags.Concat([$"course:{courseId}"]).ToArray());

        await _eventStore.AppendAsync(
            $"dcb-bootstrap:{studentId}",
            [new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, studentId, studentName, studentEmail)],
            expectedVersion: null,
            metadata: meta,
            tags: enrollmentTags);

        _sectionProjector.Apply(new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Morning Cohort", 5));
        result.Steps.Add("[Step 3] SectionCreated + StudentRegistered seeded with combined tags (student+section)");

        // ── Step 4: DCB atomic enrollment ─────────────────────────────────────
        // Load entity: reads ALL events tagged with student:{id} AND section:{id} in one query.
        // Now includes:
        //   - SectionCreated  → State.SeatsAvailable = 5
        //   - StudentRegistered → State.StudentIsActive = true
        var entity = await _dcbRepository.GetOrCreateEntityAsync<EnrollmentEntity>(enrollmentTags);

        result.Steps.Add($"[Step 4a] DCB read: {enrollmentTags.Length} tags, " +
                         $"{entity.State.SeatsAvailable} seats available, " +
                         $"student active: {entity.State.StudentIsActive}");

        // Atomic write: SeatReserved + StudentEnrolled in one AppendAsync call.
        // AppendCondition: fails if any event matching both tags appeared since we read.
        await _dcbRepository.HandleCommandAsync<EnrollmentEntity, EnrollStudentCommand>(
            entity,
            new EnrollStudentCommand(Guid.NewGuid(), studentId, courseId));

        _sectionProjector.Apply(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, sectionId, studentId));
        _transcriptProjector.Apply(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, studentId, courseId));

        result.Steps.Add("[Step 4b] DCB atomic append: SeatReserved + StudentEnrolled in 1 round-trip");
        result.Steps.Add($"          AppendCondition guards: student:{studentId} ∩ section:{sectionId}");
        result.Steps.Add($"          Seats remaining: {entity.State.SeatsAvailable - 1}");

        // ── Step 5: Send confirmation (same boundary) ─────────────────────────
        entity = await _dcbRepository.GetOrCreateEntityAsync<EnrollmentEntity>(enrollmentTags);
        await _dcbRepository.HandleCommandAsync<EnrollmentEntity, SendConfirmationCommand>(
            entity,
            new SendConfirmationCommand(Guid.NewGuid(), studentId, courseId, studentEmail));

        _transcriptProjector.Apply(new RegistrationConfirmationSent(Guid.NewGuid(), DateTime.UtcNow,
            studentId, courseId, studentEmail));
        result.Steps.Add($"[Step 5] RegistrationConfirmationSent to {studentEmail}");

        result.TotalRoundTrips = 3;
        result.ConsistencyModel = "Strong — SeatReserved + StudentEnrolled are atomic (single append)";
        result.ElapsedMs = (int)sw.ElapsedMilliseconds;
        result.SeatsRemaining = _sectionProjector.Get(sectionId)?.SeatsAvailable ?? 0;
        result.Completed = true;
        return result;
    }
}

public class ShowcaseBResult
{
    public List<string> Steps { get; set; } = [];
    public int TotalRoundTrips { get; set; }
    public string ConsistencyModel { get; set; } = string.Empty;
    public int ElapsedMs { get; set; }
    public int SeatsRemaining { get; set; }
    public bool Completed { get; set; }
}
