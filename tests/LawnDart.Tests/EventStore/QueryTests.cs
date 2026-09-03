using LawnDart.EventStore;
using Xunit;

namespace LawnDart.Tests.EventStore;

public class QueryTests
{
    [Fact]
    public void All_ReturnsEmptyQuery()
    {
        // Act
        var query = Query.All();

        // Assert
        Assert.NotNull(query);
        Assert.Empty(query.Items);
    }

    [Fact]
    public void FromItems_WithSingleItem_CreatesQuery()
    {
        // Arrange
        var item = QueryItem.ByType("TestEvent");

        // Act
        var query = Query.FromItems(item);

        // Assert
        Assert.Single(query.Items);
        Assert.Equal(item, query.Items[0]);
    }

    [Fact]
    public void FromItems_WithMultipleItems_CreatesQuery()
    {
        // Arrange
        var item1 = QueryItem.ByType("Event1");
        var item2 = QueryItem.ByTags("tag1", "tag2");

        // Act
        var query = Query.FromItems(item1, item2);

        // Assert
        Assert.Equal(2, query.Items.Count);
    }

    [Fact]
    public void FromItems_WithEmptyArray_ThrowsException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => Query.FromItems(Array.Empty<QueryItem>()));
    }

    [Fact]
    public void QueryItem_ByType_CreatesTypeQuery()
    {
        // Act
        var item = QueryItem.ByType("Event1", "Event2");

        // Assert
        Assert.NotNull(item.Types);
        Assert.Equal(2, item.Types.Count);
        Assert.Contains("Event1", item.Types);
        Assert.Contains("Event2", item.Types);
    }

    [Fact]
    public void QueryItem_ByTags_CreatesTagQuery()
    {
        // Act
        var item = QueryItem.ByTags("tag1", "tag2");

        // Assert
        Assert.NotNull(item.Tags);
        Assert.Equal(2, item.Tags.Count);
        Assert.Contains("tag1", item.Tags);
        Assert.Contains("tag2", item.Tags);
    }

    [Fact]
    public void QueryItem_ByTypeAndTags_CreatesCombinedQuery()
    {
        // Act
        var item = QueryItem.ByTypeAndTags(new[] { "Event1" }, new[] { "tag1" });

        // Assert
        Assert.NotNull(item.Types);
        Assert.NotNull(item.Tags);
        Assert.Single(item.Types);
        Assert.Single(item.Tags);
    }
}


