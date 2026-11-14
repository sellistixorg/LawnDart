using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

public class SqlServerTagConditionSqlTests
{
    [Fact]
    public void Empty_after_tag_condition_reads_the_tags_table()
    {
        var (sql, _) = Store().PreviewAppendCondition(
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("course:1"))));

        Assert.Contains("EventTags", sql, StringComparison.Ordinal);
        Assert.Contains("TagId", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GlobalSequencePosition >", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("[Events]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Tag_and_type_condition_filters_the_tag_row()
    {
        var (sql, _) = Store().PreviewAppendCondition(
            AppendCondition.FailIfMatches(Query.FromItems(
                QueryItem.ByTypeAndTags(["note-added"], ["course:1"]))));

        Assert.Contains("et0.EventTypeId IN", sql, StringComparison.Ordinal);
        Assert.Contains("TagRegistry", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("[Events]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Several_tags_and_any_of_stay_on_the_tags_table()
    {
        var (sql, _) = Store().PreviewAppendCondition(
            AppendCondition.FailIfMatches(Query.FromItems(
                QueryItem.ByTags("course:1", "student:1"),
                QueryItem.ByTags("course:2"))));

        Assert.Contains("et1.TagId", sql, StringComparison.Ordinal);
        Assert.Contains(" OR ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("[Events]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Recent_after_still_bounds_the_tag_position()
    {
        var (sql, _) = Store().PreviewAppendCondition(
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("course:1")), after: 40));

        Assert.Contains("et0.GlobalSequencePosition > @After", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("[Events]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Types_only_condition_reads_events()
    {
        var (sql, _) = Store().PreviewAppendCondition(
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("note-added"))));

        Assert.Contains("[Events]", sql, StringComparison.Ordinal);
        Assert.Contains("e.EventTypeId IN", sql, StringComparison.Ordinal);
    }

    private static SqlServerEventStore Store()
        => new("Server=localhost;Database=test;");
}
