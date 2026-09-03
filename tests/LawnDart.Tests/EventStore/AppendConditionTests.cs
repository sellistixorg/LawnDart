using LawnDart.EventStore;
using Xunit;

namespace LawnDart.Tests.EventStore;

public class AppendConditionTests
{
    [Fact]
    public void FailIfMatches_CreatesCondition()
    {
        // Arrange
        var query = Query.FromItems(QueryItem.ByType("TestEvent"));

        // Act
        var condition = AppendCondition.FailIfMatches(query, 100);

        // Assert
        Assert.NotNull(condition);
        Assert.Equal(query, condition.FailIfEventsMatch);
        Assert.Equal(100, condition.After);
    }

    [Fact]
    public void FailIfMatches_WithoutAfter_CreatesCondition()
    {
        // Arrange
        var query = Query.All();

        // Act
        var condition = AppendCondition.FailIfMatches(query);

        // Assert
        Assert.NotNull(condition);
        Assert.Null(condition.After);
    }
}


