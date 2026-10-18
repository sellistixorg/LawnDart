using LawnDart.Tagging;

namespace LawnDart.Tests.Tagging;

public class TagProviderTests
{
    [Fact]
    public void Provider_BuildsGetTagsAndHelpersThroughTheSameFormat()
    {
        var format = LowercaseTagFormat.Instance;
        var provider = new AcademyTags(format);
        var studentId = Guid.Parse("0ad2f06e-fc65-4394-bfa1-f3116bf68808");
        var sectionId = Guid.Parse("f81ccc2f-61e6-46da-bcb4-d8eec7f0220e");

        var enrollment = provider.ForEnrollment(studentId, sectionId);
        var fromEvent = provider.GetTags(new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, studentId));

        Assert.Equal(
            [format.Tag("student", studentId), format.Tag("section", sectionId)],
            enrollment);
        Assert.Equal([format.Tag("student", studentId)], fromEvent);
        Assert.Equal(format.Tag("course", sectionId), provider.Tag("course", sectionId));
    }

    [Fact]
    public void Provider_UsesTheFormatItWasGiven()
    {
        var provider = new AcademyTags(new MarkedFormat("academy"));
        var studentId = Guid.Parse("0ad2f06e-fc65-4394-bfa1-f3116bf68808");

        Assert.Equal(
            $"academy:student:{studentId:D}",
            provider.Tag("Student", studentId));
    }

    [Fact]
    public void Provider_RejectsNullFormat()
    {
        Assert.Throws<ArgumentNullException>(() => new AcademyTags(null!));
    }

    private sealed class AcademyTags : TagProvider
    {
        public AcademyTags(ITagFormat format) : base(format) { }

        public string[] ForEnrollment(Guid studentId, Guid sectionId)
            => [Tag("student", studentId), Tag("section", sectionId)];

        public override IEnumerable<string> GetTags(IEvent @event, object? context = null)
            => @event is StudentRegistered registered
                ? [Tag("student", registered.StudentId)]
                : [];
    }

    private sealed class MarkedFormat : ITagFormat
    {
        private readonly string _mark;

        public MarkedFormat(string mark) => _mark = mark;

        public string Tag(string type, Guid id) => Tag(type, id.ToString("D"));

        public string Tag(string type, string id) => $"{_mark}:{type.ToLowerInvariant()}:{id}";
    }

    private sealed record StudentRegistered(Guid Id, DateTime Timestamp, Guid StudentId) : IEvent;
}
