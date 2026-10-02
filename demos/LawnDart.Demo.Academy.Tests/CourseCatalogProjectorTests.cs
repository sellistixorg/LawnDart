using LawnDart.Demo.Academy.Domain.Course;
using LawnDart.Demo.Academy.Domain.Course.Commands;
using LawnDart.Demo.Academy.Domain.Course.Events;
using LawnDart.Demo.Academy.Projections;
using LawnDart.Testing.Bdd;

namespace LawnDart.Demo.Academy.Tests;

public sealed class CourseCatalogProjectorTests
{
    [Fact]
    public async Task create_keeps_a_draft_out_of_the_published_catalog()
    {
        await using var ctx = BddTestContext.CreateInMemory(
            typeof(CourseCreated), typeof(CoursePublished), typeof(CoursePriceUpdated));
        var courseId = Guid.NewGuid();
        var projector = new CourseCatalogProjector();

        await AggregateSpec
            .For<Course>(ctx, courseId)
            .When(new CreateCourseCommand(
                Guid.NewGuid(), courseId, "Event Modeling", "From slice to code", 149m))
            .ThenEmittedEvent<CourseCreated>(e =>
                e.CourseId == courseId && e.Title == "Event Modeling" && e.Price == 149m)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                Assert.Empty(projector.GetPublished());

                var view = projector.Get(courseId);
                Assert.NotNull(view);
                Assert.Equal("Event Modeling", view.Title);
                Assert.Equal("From slice to code", view.Description);
                Assert.Equal(149m, view.Price);
                Assert.Equal(CourseStatus.Draft, view.Status);
            })
            .RunAsync();
    }

    [Fact]
    public async Task publish_lists_the_course_at_its_current_price()
    {
        await using var ctx = BddTestContext.CreateInMemory(
            typeof(CourseCreated), typeof(CoursePublished), typeof(CoursePriceUpdated));
        var courseId = Guid.NewGuid();
        var draftId = Guid.NewGuid();
        var projector = new CourseCatalogProjector();

        await AggregateSpec
            .For<Course>(ctx, courseId)
            .Given(new CourseCreated(
                Guid.NewGuid(), DateTime.UtcNow, courseId, "Event Modeling", "From slice to code", 149m))
            .When(new PublishCourseCommand(Guid.NewGuid(), courseId))
            .ThenEmittedEvent<CoursePublished>(e => e.CourseId == courseId)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                var listed = Assert.Single(projector.GetPublished());
                Assert.Equal(courseId, listed.CourseId);
                Assert.Equal("Event Modeling", listed.Title);
                Assert.Equal("From slice to code", listed.Description);
                Assert.Equal(149m, listed.Price);
                Assert.Equal(CourseStatus.Published, listed.Status);
            })
            .RunAsync();

        projector.Apply(new CourseCreated(
            Guid.NewGuid(), DateTime.UtcNow, draftId, "Still a draft", "Not listed", 10m));

        var published = Assert.Single(projector.GetPublished());
        Assert.Equal(courseId, published.CourseId);
        Assert.Equal(CourseStatus.Draft, projector.Get(draftId)!.Status);
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
                    Guid.NewGuid(), DateTime.UtcNow, courseId, "Event Modeling", "From slice to code", 149m),
                new CoursePublished(Guid.NewGuid(), DateTime.UtcNow, courseId))
            .When(new UpdateCoursePriceCommand(Guid.NewGuid(), courseId, 199m))
            .ThenEmittedEvent<CoursePriceUpdated>(e => e.CourseId == courseId && e.NewPrice == 199m)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                var listed = Assert.Single(projector.GetPublished());
                Assert.Equal(199m, listed.Price);
                Assert.Equal(CourseStatus.Published, listed.Status);
                Assert.Equal("Event Modeling", listed.Title);
            })
            .RunAsync();
    }

    [Fact]
    public async Task price_update_before_publish_is_the_listed_price()
    {
        await using var ctx = BddTestContext.CreateInMemory(
            typeof(CourseCreated), typeof(CoursePublished), typeof(CoursePriceUpdated));
        var courseId = Guid.NewGuid();
        var projector = new CourseCatalogProjector();

        await AggregateSpec
            .For<Course>(ctx, courseId)
            .Given(
                new CourseCreated(
                    Guid.NewGuid(), DateTime.UtcNow, courseId, "Event Modeling", "From slice to code", 149m),
                new CoursePriceUpdated(Guid.NewGuid(), DateTime.UtcNow, courseId, 175m))
            .When(new PublishCourseCommand(Guid.NewGuid(), courseId))
            .ThenEmittedEvent<CoursePublished>(e => e.CourseId == courseId)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(_ =>
            {
                var listed = Assert.Single(projector.GetPublished());
                Assert.Equal(175m, listed.Price);
                Assert.Equal(CourseStatus.Published, listed.Status);
            })
            .RunAsync();
    }

    [Fact]
    public void publish_or_price_for_an_unknown_course_is_ignored()
    {
        var projector = new CourseCatalogProjector();
        var courseId = Guid.NewGuid();

        projector.Apply(new CoursePublished(Guid.NewGuid(), DateTime.UtcNow, courseId));
        projector.Apply(new CoursePriceUpdated(Guid.NewGuid(), DateTime.UtcNow, courseId, 80m));

        Assert.Null(projector.Get(courseId));
        Assert.Empty(projector.GetPublished());
    }
}
