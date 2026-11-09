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
/// Showcase B: DCB in one process. No broker.
/// Register the student, create the course, then seed the section and the
/// student with their own tags. Enrollment loads
/// <see cref="EnrollmentEntity.EnrollmentDecision"/>, then appends
/// SeatReserved and StudentEnrolled in one write. The same query is the
/// append condition, so a seat taken after the read fails the append.
/// Confirmation uses that query again.
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

        // Each seeded event carries only its own tags. The section item of the
        // decision finds SectionCreated. The student item finds StudentRegistered.
        var decision = EnrollmentEntity.EnrollmentDecision(studentId, sectionId);
        var meta = new LawnDart.Metadata.EventMetadata { Timestamp = DateTime.UtcNow };

        await _eventStore.AppendAsync(
            $"dcb-bootstrap:{sectionId}",
            [new TaggedEvent(
                new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Morning Cohort", 5),
                [$"section:{sectionId}", $"course:{courseId}"])],
            expectedVersion: null,
            metadata: meta);

        await _eventStore.AppendAsync(
            $"dcb-bootstrap:{studentId}",
            [new TaggedEvent(
                new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, studentId, studentName, studentEmail),
                [$"student:{studentId}"])],
            expectedVersion: null,
            metadata: meta);

        _sectionProjector.Apply(new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Morning Cohort", 5));
        result.Steps.Add("[Step 3] SectionCreated seeded with section and course tags. StudentRegistered seeded with the student tag.");

        // The section item loads every seat event. The student item loads this student's registration.
        var entity = await _dcbRepository.GetOrCreateEntityAsync<EnrollmentEntity>(decision);

        result.Steps.Add($"[Step 4a] DCB read: {entity.State.SeatsAvailable} seats available, " +
                         $"student active: {entity.State.StudentIsActive}");

        // SeatReserved and StudentEnrolled share one append. The condition is the decision query.
        await _dcbRepository.HandleCommandAsync<EnrollmentEntity, EnrollStudentCommand>(
            entity,
            new EnrollStudentCommand(Guid.NewGuid(), studentId, courseId));

        _sectionProjector.Apply(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, sectionId, studentId));
        _transcriptProjector.Apply(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, studentId, courseId));

        result.Steps.Add("[Step 4b] DCB atomic append: SeatReserved + StudentEnrolled in 1 round-trip");
        result.Steps.Add("          Append condition uses the enrollment decision");
        result.Steps.Add($"          Seats remaining: {entity.State.SeatsAvailable}");

        entity = await _dcbRepository.GetOrCreateEntityAsync<EnrollmentEntity>(decision);
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
