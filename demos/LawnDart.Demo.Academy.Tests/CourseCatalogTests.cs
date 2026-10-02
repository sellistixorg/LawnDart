using LawnDart.Demo.Academy.Domain.Course;
using LawnDart.Demo.Academy.Domain.Course.Commands;
using LawnDart.Demo.Academy.Domain.Course.Events;
using LawnDart.Demo.Academy.Projections;
using LawnDart.Testing.Bdd;

namespace LawnDart.Demo.Academy.Tests;

public sealed class CourseCatalogTests
{
    [Fact]
    public async Task create_course_stays_out_of_the_published_catalog()
    {
        await using var ctx = BddTestContext.CreateInMemory(
            typeof(CourseCreated), typeof(CoursePublished), typeof(CoursePriceUpdated));
        var courseId = Guid.NewGuid();
        var projector = new CourseCatalogProjector();

        await AggregateSpec
            .For<Course>(ctx, courseId)
            .When(new CreateCourseCommand(
                Guid.NewGuid(), courseId, "Event Modeling", "Draft syllabus", 199m))
            .ThenEmittedEvent<CourseCreated>(e =>
                e.CourseId == courseId && e.Title == "Event Modeling" && e.Price == 199m)
            .AndExpectedVersion(1)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                Assert.Empty(projector.ListPublished());

                var row = projector.Get(courseId);
                Assert.NotNull(row);
                Assert.Equal("Event Modeling", row.Title);
                Assert.Equal("Draft syllabus", row.Description);
                Assert.Equal(199m, row.Price);
                Assert.Equal(CourseStatus.Draft, row.Status);
            })
            .RunAsync();
    }

    [Fact]
    public async Task publish_lists_the_course_with_its_current_price()
    {
        await using var ctx = BddTestContext.CreateInMemory(
            typeof(CourseCreated), typeof(CoursePublished), typeof(CoursePriceUpdated));
        var courseId = Guid.NewGuid();
        var projector = new CourseCatalogProjector();

        await AggregateSpec
            .For<Course>(ctx, courseId)
            .Given(new CourseCreated(
                Guid.NewGuid(), DateTime.UtcNow, courseId, "Event Modeling", "Live syllabus", 199m))
            .When(new PublishCourseCommand(Guid.NewGuid(), courseId))
            .ThenEmittedEvent<CoursePublished>(e => e.CourseId == courseId)
            .AndExpectedVersion(2)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                var row = Assert.Single(projector.ListPublished());
                Assert.Equal(courseId, row.CourseId);
                Assert.Equal("Event Modeling", row.Title);
                Assert.Equal("Live syllabus", row.Description);
                Assert.Equal(199m, row.Price);
                Assert.Equal(CourseStatus.Published, row.Status);
            })
            .RunAsync();
    }

    [Fact]
    public async Task price_update_replaces_the_listed_price()
    {
        await using var ctx = BddTestContext.CreateInMemory(
            typeof(CourseCreated), typeof(CoursePublished), typeof(CoursePriceUpdated));
        var courseId = Guid.NewGuid();
        var projector = new CourseCatalogProjector();

        await AggregateSpec
            .For<Course>(ctx, courseId)
            .Given(
                new CourseCreated(
                    Guid.NewGuid(), DateTime.UtcNow, courseId, "Event Modeling", "Live syllabus", 199m),
                new CoursePublished(Guid.NewGuid(), DateTime.UtcNow, courseId))
            .When(new UpdateCoursePriceCommand(Guid.NewGuid(), courseId, 249m))
            .ThenEmittedEvent<CoursePriceUpdated>(e => e.CourseId == courseId && e.NewPrice == 249m)
            .AndExpectedVersion(3)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                var row = Assert.Single(projector.ListPublished());
                Assert.Equal(courseId, row.CourseId);
                Assert.Equal(249m, row.Price);
                Assert.Equal(CourseStatus.Published, row.Status);
            })
            .RunAsync();
    }

    [Fact]
    public void published_catalog_omits_drafts_and_keeps_a_price_set_before_publish()
    {
        var projector = new CourseCatalogProjector();
        var draftId = Guid.NewGuid();
        var liveId = Guid.NewGuid();
        var now = DateTime.UtcNow;

        projector.Apply(new CourseCreated(Guid.NewGuid(), now, draftId, "Draft Course", "Hidden", 10m));
        projector.Apply(new CourseCreated(Guid.NewGuid(), now, liveId, "Live Course", "Visible", 20m));
        projector.Apply(new CoursePriceUpdated(Guid.NewGuid(), now, liveId, 25m));
        projector.Apply(new CoursePublished(Guid.NewGuid(), now, liveId));

        var row = Assert.Single(projector.ListPublished());
        Assert.Equal(liveId, row.CourseId);
        Assert.Equal("Live Course", row.Title);
        Assert.Equal("Visible", row.Description);
        Assert.Equal(25m, row.Price);
        Assert.Equal(CourseStatus.Published, row.Status);

        var draft = projector.Get(draftId);
        Assert.NotNull(draft);
        Assert.Equal(CourseStatus.Draft, draft.Status);
        Assert.Equal(10m, draft.Price);
    }
}
