using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

public class DcbQuerySqlTests
{
    [Fact]
    public void Two_tag_query_intersects_EventTags_and_never_reads_e_Tags()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            Query.FromItems(QueryItem.ByTags("alpha", "beta")),
            _ => null,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.False(built.ShortCircuited);
        Assert.Contains("INNER JOIN [dbo].[EventTags]", built.Sql, StringComparison.Ordinal);
        Assert.Contains("[dbo].[EventTags] et0_0", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("OPENJSON", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("OPENJSON(e.Tags)", built.Sql, StringComparison.Ordinal);
        var where = built.Sql.Contains("WHERE", StringComparison.Ordinal)
            ? built.Sql[built.Sql.IndexOf("WHERE", StringComparison.Ordinal)..]
            : built.Sql;
        Assert.DoesNotContain("e.Tags", where, StringComparison.Ordinal);
    }

    [Fact]
    public void Type_query_uses_event_type_id_in_list_and_has_no_like()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            Query.FromItems(QueryItem.ByType("order-placed", "order-cancelled")),
            token => token switch
            {
                "order-placed" => 7,
                "order-cancelled" => 11,
                _ => null
            },
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.False(built.ShortCircuited);
        Assert.Contains("e.EventTypeId IN (@Type0_0, @Type0_1)", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIKE", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("e.EventType ", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("e.EventType=", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NVARCHAR", built.Sql, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(7, built.Parameters.Single(p => p.ParameterName == "@Type0_0").Value);
        Assert.Equal(11, built.Parameters.Single(p => p.ParameterName == "@Type0_1").Value);
        Assert.All(built.Parameters, p => Assert.IsNotType<string>(p.Value));
    }

    [Fact]
    public void Unknown_type_item_short_circuits_without_sql()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            Query.FromItems(QueryItem.ByType("no-such-family")),
            _ => null,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.True(built.ShortCircuited);
        Assert.Equal("", built.Sql);
        Assert.Empty(built.Parameters);
    }
}
