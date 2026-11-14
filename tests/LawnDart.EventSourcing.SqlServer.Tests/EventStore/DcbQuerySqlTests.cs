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
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByTags("alpha", "beta")),
            _ => null,
            TagId,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.False(built.ShortCircuited);
        Assert.Contains("INNER JOIN [dbo].[EventTags]", built.Sql, StringComparison.Ordinal);
        Assert.Contains("[dbo].[EventTags] et0_0", built.Sql, StringComparison.Ordinal);
        Assert.Contains("et0_0.TagId = @Tag0_0", built.Sql, StringComparison.Ordinal);
        Assert.Contains("et0_1.TagId = @Tag0_1", built.Sql, StringComparison.Ordinal);
        Assert.Equal(1, built.Parameters.Single(p => p.ParameterName == "@Tag0_0").Value);
        Assert.Equal(2, built.Parameters.Single(p => p.ParameterName == "@Tag0_1").Value);
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
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByType("order-placed", "order-cancelled")),
            token => token switch
            {
                "order-placed" => 7,
                "order-cancelled" => 11,
                _ => null
            },
            _ => null,
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
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByType("no-such-family")),
            _ => null,
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

    [Fact]
    public void Reads_stop_at_the_stable_rowversion_horizon()
    {
        var all = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.All(),
            _ => null,
            _ => null,
            fromSequencePosition: 1,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);
        Assert.Contains(DcbQuerySql.StableHorizon, all.Sql, StringComparison.Ordinal);

        var tags = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByTags("Customer:ABC")),
            _ => null,
            TagId,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);
        Assert.Contains(DcbQuerySql.StableHorizon, tags.Sql, StringComparison.Ordinal);

        var max = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByType("order-placed")),
            token => token == "order-placed" ? 4 : null,
            _ => null,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.MaxSequence);
        Assert.Contains("MIN_ACTIVE_ROWVERSION()", max.Sql, StringComparison.Ordinal);

        var advanced = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.All(),
            _ => null,
            _ => null,
            fromSequencePosition: 1,
            toSequencePosition: 4,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events,
            applyStableHorizon: false);
        Assert.DoesNotContain("MIN_ACTIVE_ROWVERSION()", advanced.Sql, StringComparison.Ordinal);
        Assert.Contains("READPAST", advanced.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Tag_and_type_filters_the_tag_row_and_fetches_events_by_position()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByTypeAndTags(["note-added"], ["sku:0"])),
            token => token == "note-added" ? 4 : null,
            TagId,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: 50,
            DcbQuerySql.Mode.Events);

        Assert.Contains("et0_0.TagId = @Tag0_0", built.Sql, StringComparison.Ordinal);
        Assert.Contains("et0_0.EventTypeId IN (@Type0_0)", built.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("e.EventTypeId IN", built.Sql, StringComparison.Ordinal);
        Assert.Contains("FORCESEEK", built.Sql, StringComparison.Ordinal);
        Assert.Contains("e.SequencePosition = et0_0.GlobalSequencePosition", built.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_tag_item_short_circuits_without_sql()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByTags("missing")),
            _ => null,
            _ => null,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.True(built.ShortCircuited);
        Assert.Equal("", built.Sql);
    }

    [Fact]
    public void Any_of_items_union_tag_positions()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            "[dbo].[Streams]",
            Query.FromItems(QueryItem.ByTags("alpha"), QueryItem.ByTags("beta")),
            _ => null,
            TagId,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.Contains("UNION", built.Sql, StringComparison.Ordinal);
        Assert.Contains("et0_0.TagId = @Tag0_0", built.Sql, StringComparison.Ordinal);
        Assert.Contains("et1_0.TagId = @Tag1_0", built.Sql, StringComparison.Ordinal);
        Assert.Contains("FORCESEEK", built.Sql, StringComparison.Ordinal);
    }

    private static int? TagId(string tag) => tag switch
    {
        "alpha" => 1,
        "beta" => 2,
        "Customer:ABC" => 3,
        "sku:0" => 4,
        _ => null
    };
}
