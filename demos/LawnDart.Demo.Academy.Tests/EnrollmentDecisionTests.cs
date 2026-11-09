using LawnDart.Dcb;
using LawnDart.Demo.Academy.Domain.Course.Events;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Enrollment;
using LawnDart.Demo.Academy.Projections;
using LawnDart.Demo.Academy.Showcases;
using LawnDart.EventStore;
using LawnDart.Testing.Bdd;

namespace LawnDart.Demo.Academy.Tests;

public sealed class EnrollmentDecisionTests
{
    private static readonly Type[] EventTypes =
    [
        typeof(StudentRegistered),
        typeof(StudentPaymentProcessed),
        typeof(StudentEnrolled),
        typeof(RegistrationConfirmationSent),
        typeof(RegistrationCancelled),
        typeof(SectionCreated),
        typeof(SeatReserved),
        typeof(SeatReleased),
        typeof(CourseCreated),
    ];

    private static readonly EventTypeCatalog Catalog = EventTypeCatalog.Materialize(EventTypes);

    [Fact]
    public void enrollment_decision_names_the_student_events()
    {
        var studentId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();
        var query = EnrollmentEntity.EnrollmentDecision(studentId, sectionId);

        Assert.Equal(
            "StudentRegistered",
            EventTypeCatalog.TryGetDeclaredName(typeof(StudentRegistered)));
        Assert.Equal(
            "StudentEnrolled",
            EventTypeCatalog.TryGetDeclaredName(typeof(StudentEnrolled)));
        Assert.Equal(
            "RegistrationConfirmationSent",
            EventTypeCatalog.TryGetDeclaredName(typeof(RegistrationConfirmationSent)));
        Assert.Equal(
            "RegistrationCancelled",
            EventTypeCatalog.TryGetDeclaredName(typeof(RegistrationCancelled)));

        Assert.Equal(2, query.Items.Count);
        Assert.Null(query.Items[0].Types);
        Assert.Equal(new[] { $"section:{sectionId}" }, query.Items[0].Tags);
        Assert.Equal(
            new[]
            {
                "StudentRegistered",
                "StudentEnrolled",
                "RegistrationConfirmationSent",
                "RegistrationCancelled",
            },
            query.Items[1].Types);
        Assert.Equal(new[] { $"student:{studentId}" }, query.Items[1].Tags);
        Assert.DoesNotContain("SeatReserved", query.Items[1].Types!);
        Assert.DoesNotContain("SeatReleased", query.Items[1].Types!);
        Assert.DoesNotContain("StudentPaymentProcessed", query.Items[1].Types!);
    }

    [Fact]
    public async Task one_seat_left_admits_exactly_one_student()
    {
        await using var ctx = Open();
        var courseId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();
        var studentA = Guid.NewGuid();
        var studentB = Guid.NewGuid();
        await SeedSectionAsync(ctx, sectionId, courseId, seats: 1);
        await SeedStudentAsync(ctx, studentA, "Ada");
        await SeedStudentAsync(ctx, studentB, "Grace");

        var first = await LoadAsync(ctx.DcbRepository, studentA, sectionId);
        Assert.True(first.State.StudentIsActive);
        Assert.Equal(1, first.State.SeatsAvailable);
        await EnrollAsync(ctx.DcbRepository, first, studentA, courseId);
        Assert.True(first.State.StudentEnrolled);
        Assert.Equal(0, first.State.SeatsAvailable);

        var second = await LoadAsync(ctx.DcbRepository, studentB, sectionId);
        Assert.True(second.State.StudentIsActive);
        Assert.Equal(studentB, second.State.StudentId);
        Assert.Equal(0, second.State.SeatsAvailable);
        Assert.False(second.State.StudentEnrolled);
        Assert.False(second.State.SeatReserved);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            EnrollAsync(ctx.DcbRepository, second, studentB, courseId));
        Assert.Contains("No seats available", refused.Message, StringComparison.Ordinal);

        var stored = await AllAsync(ctx);
        Assert.Single(stored, e => e.Event is SeatReserved);
        var enrolled = Assert.Single(stored, e => e.Event is StudentEnrolled);
        Assert.Equal(studentA, Assert.IsType<StudentEnrolled>(enrolled.Event).StudentId);

        var decisionA = EnrollmentEntity.EnrollmentDecision(studentA, sectionId);
        var decisionB = EnrollmentEntity.EnrollmentDecision(studentB, sectionId);
        Assert.All(
            stored.Where(e => e.Event is SeatReserved or StudentEnrolled),
            e =>
            {
                Assert.True(EventQueryMatcher.Matches(e, decisionA, Catalog));
                Assert.True(EventQueryMatcher.Matches(e, decisionB, Catalog));
            });

        var registrationA = Assert.Single(
            stored,
            e => e.Event is StudentRegistered registered && registered.StudentId == studentA);
        Assert.Equal(new[] { $"student:{studentA}" }, registrationA.Tags);
        Assert.True(EventQueryMatcher.Matches(registrationA, decisionA, Catalog));
        Assert.False(EventQueryMatcher.Matches(registrationA, decisionB, Catalog));
    }

    [Fact]
    public async Task concurrent_last_seat_conflicts()
    {
        await using var ctx = Open();
        var courseId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();
        var studentA = Guid.NewGuid();
        var studentB = Guid.NewGuid();
        await SeedSectionAsync(ctx, sectionId, courseId, seats: 1);
        await SeedStudentAsync(ctx, studentA, "Ada");
        await SeedStudentAsync(ctx, studentB, "Grace");

        var entityA = await LoadAsync(ctx.DcbRepository, studentA, sectionId);
        var entityB = await LoadAsync(ctx.DcbRepository, studentB, sectionId);
        Assert.Equal(1, entityA.State.SeatsAvailable);
        Assert.Equal(1, entityB.State.SeatsAvailable);

        var outcomes = await Task.WhenAll(
            TryEnrollAsync(ctx.DcbRepository, entityA, studentA, courseId),
            TryEnrollAsync(ctx.DcbRepository, entityB, studentB, courseId));

        Assert.Equal(1, outcomes.Count(outcome => outcome is null));
        Assert.IsType<ConcurrencyException>(Assert.Single(outcomes, outcome => outcome is not null));

        var againA = await LoadAsync(ctx.DcbRepository, studentA, sectionId);
        var againB = await LoadAsync(ctx.DcbRepository, studentB, sectionId);
        Assert.Equal(0, againA.State.SeatsAvailable);
        Assert.Equal(0, againB.State.SeatsAvailable);
        Assert.Equal(1, new[] { againA, againB }.Count(entity => entity.State.StudentEnrolled));
        Assert.Single(await AllAsync(ctx), e => e.Event is SeatReserved);
    }

    [Fact]
    public async Task seat_count_includes_another_students_reservation()
    {
        await using var ctx = Open();
        var courseId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();
        var studentA = Guid.NewGuid();
        var studentB = Guid.NewGuid();
        await SeedSectionAsync(ctx, sectionId, courseId, seats: 5);
        await SeedStudentAsync(ctx, studentA, "Ada");
        await SeedStudentAsync(ctx, studentB, "Grace");

        var first = await LoadAsync(ctx.DcbRepository, studentA, sectionId);
        await EnrollAsync(ctx.DcbRepository, first, studentA, courseId);
        await ctx.DcbRepository.HandleCommandAsync(
            first,
            new SendConfirmationCommand(Guid.NewGuid(), studentA, courseId, "ada@academy.example"));

        var second = await LoadAsync(ctx.DcbRepository, studentB, sectionId);
        Assert.Equal(4, second.State.SeatsAvailable);
        Assert.True(second.State.StudentIsActive);
        Assert.Equal(studentB, second.State.StudentId);
        Assert.False(second.State.StudentEnrolled);
        Assert.False(second.State.SeatReserved);
        Assert.False(second.State.ConfirmationSent);
        Assert.False(second.State.IsCancelled);

        var reserved = Assert.Single((await AllAsync(ctx)), e => e.Event is SeatReserved);
        Assert.Contains($"section:{sectionId}", reserved.Tags);
        Assert.DoesNotContain($"student:{studentB}", reserved.Tags);
        Assert.True(EventQueryMatcher.Matches(
            reserved,
            EnrollmentEntity.EnrollmentDecision(studentB, sectionId),
            Catalog));
    }

    [Fact]
    public async Task another_students_release_returns_the_seat()
    {
        await using var ctx = Open();
        var courseId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();
        var studentId = Guid.NewGuid();
        var otherId = Guid.NewGuid();
        await SeedSectionAsync(ctx, sectionId, courseId, seats: 2);
        await SeedStudentAsync(ctx, studentId, "Ada");
        await ctx.EventStore.AppendAsync(
            $"seat:{Guid.NewGuid():N}",
            [new TaggedEvent(
                new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, sectionId, studentId),
                [$"student:{studentId}", $"section:{sectionId}"])]);
        await ctx.EventStore.AppendAsync(
            $"seat:{Guid.NewGuid():N}",
            [new TaggedEvent(
                new SeatReleased(Guid.NewGuid(), DateTime.UtcNow, sectionId, otherId),
                [$"student:{otherId}", $"section:{sectionId}"])]);
        await ctx.EventStore.AppendAsync(
            $"seat:{Guid.NewGuid():N}",
            [new TaggedEvent(
                new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, otherId, courseId),
                [$"student:{otherId}", $"section:{sectionId}", $"course:{courseId}"])]);

        var entity = await LoadAsync(ctx.DcbRepository, studentId, sectionId);
        Assert.Equal(2, entity.State.SeatsAvailable);
        Assert.True(entity.State.SeatReserved);
        Assert.False(entity.State.StudentEnrolled);
        Assert.False(entity.State.ConfirmationSent);
        Assert.False(entity.State.IsCancelled);
    }

    [Fact]
    public async Task seeded_events_carry_only_their_own_tags()
    {
        await using var ctx = Open();
        var showcase = new ShowcaseB_DcbInProcess(
            ctx.AggregateRepository,
            ctx.DcbRepository,
            ctx.EventStore,
            new CourseSectionProjector(),
            new StudentTranscriptProjector());

        var studentId = Guid.NewGuid();
        var courseId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();
        var result = await showcase.RunAsync(studentId, courseId, sectionId, "Ada Lovelace");
        Assert.True(result.Completed);
        Assert.Equal(4, result.SeatsRemaining);

        var stored = await AllAsync(ctx);
        var sectionSeed = Assert.Single(stored, e => e.StreamId == $"dcb-bootstrap:{sectionId}");
        Assert.IsType<SectionCreated>(sectionSeed.Event);
        Assert.Equal(new[] { $"section:{sectionId}", $"course:{courseId}" }, sectionSeed.Tags);

        var studentSeed = Assert.Single(stored, e => e.StreamId == $"dcb-bootstrap:{studentId}");
        var registered = Assert.IsType<StudentRegistered>(studentSeed.Event);
        Assert.Equal(studentId, registered.StudentId);
        Assert.Equal(new[] { $"student:{studentId}" }, studentSeed.Tags);

        var decision = EnrollmentEntity.EnrollmentDecision(studentId, sectionId);
        Assert.True(EventQueryMatcher.Matches(sectionSeed, decision, Catalog));
        Assert.True(EventQueryMatcher.Matches(studentSeed, decision, Catalog));
        Assert.All(
            stored.Where(e => e.StreamId.StartsWith("dcb:", StringComparison.Ordinal)),
            e => Assert.True(EventQueryMatcher.Matches(e, decision, Catalog), e.Event.GetType().Name));
    }

    private static BddTestContext Open() => BddTestContext.CreateInMemory(EventTypes);

    private static async Task SeedSectionAsync(BddTestContext ctx, Guid sectionId, Guid courseId, int seats)
    {
        await ctx.EventStore.AppendAsync(
            $"dcb-bootstrap:{sectionId}",
            [new TaggedEvent(
                new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Morning Cohort", seats),
                [$"section:{sectionId}", $"course:{courseId}"])]);
    }

    private static async Task SeedStudentAsync(BddTestContext ctx, Guid studentId, string name)
    {
        await ctx.EventStore.AppendAsync(
            $"dcb-bootstrap:{studentId}",
            [new TaggedEvent(
                new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, studentId, name, "student@academy.example"),
                [$"student:{studentId}"])]);
    }

    private static Task<EnrollmentEntity> LoadAsync(IDcbRepository repository, Guid studentId, Guid sectionId) =>
        repository.GetOrCreateEntityAsync<EnrollmentEntity>(
            EnrollmentEntity.EnrollmentDecision(studentId, sectionId));

    private static Task EnrollAsync(IDcbRepository repository, EnrollmentEntity entity, Guid studentId, Guid courseId) =>
        repository.HandleCommandAsync(entity, new EnrollStudentCommand(Guid.NewGuid(), studentId, courseId));

    private static async Task<Exception?> TryEnrollAsync(
        IDcbRepository repository,
        EnrollmentEntity entity,
        Guid studentId,
        Guid courseId)
    {
        try
        {
            await EnrollAsync(repository, entity, studentId, courseId);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static async Task<IReadOnlyList<SequencedEvent>> AllAsync(BddTestContext ctx) =>
        (await ctx.EventStore.ReadByQueryAsync(Query.All())).Events;
}
