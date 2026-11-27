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
        var item2 = QueryItem.ByTags(Tag.Parse("tag:tag1"), Tag.Parse("tag:tag2"));

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
        var item = QueryItem.ByTags(Tag.Parse("tag:tag1"), Tag.Parse("tag:tag2"));

        // Assert
        Assert.NotNull(item.Tags);
        Assert.Equal(2, item.Tags.Count);
        Assert.Contains(Tag.Parse("tag:tag1"), item.Tags);
        Assert.Contains(Tag.Parse("tag:tag2"), item.Tags);
    }

    [Fact]
    public void QueryItem_ByTypeAndTags_CreatesCombinedQuery()
    {
        // Act
        var item = QueryItem.ByTypeAndTags(new[] { "Event1" }, new[] { Tag.Parse("tag:tag1") });

        // Assert
        Assert.NotNull(item.Types);
        Assert.NotNull(item.Tags);
        Assert.Single(item.Types);
        Assert.Single(item.Tags);
    }
}



public class QueryTenancyTests
{
    [Fact]
    public void ForTenant_SetsTenantId_WithoutMutatingOriginal()
    {
        var original = Query.FromItems(QueryItem.ByType("OrderPlaced"));
        var scoped = original.ForTenant("acme");

        Assert.Null(original.TenantId);
        Assert.False(original.AllTenants);
        Assert.Equal("acme", scoped.TenantId);
        Assert.False(scoped.AllTenants);
        Assert.Equal(original.Items, scoped.Items);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(" acme")]
    [InlineData("acme ")]
    [InlineData("acme:sub")]
    public void ForTenant_RejectsInvalidTenantIds(string tenantId)
    {
        var query = Query.All();
        Assert.Throws<ArgumentException>(() => query.ForTenant(tenantId));
    }

    [Fact]
    public void ForTenant_ThrowsOnAllTenantsQuery()
    {
        var query = Query.ForAllTenants();
        Assert.Throws<InvalidOperationException>(() => query.ForTenant("acme"));
    }

    [Fact]
    public void ForAllTenants_SetsAllTenantsTrueAndLeavesTenantIdNull()
    {
        var q1 = Query.ForAllTenants();
        Assert.True(q1.AllTenants);
        Assert.Null(q1.TenantId);
        Assert.Empty(q1.Items);

        var item = QueryItem.ByType("OrderPlaced");
        var q2 = Query.ForAllTenants(item);
        Assert.True(q2.AllTenants);
        Assert.Null(q2.TenantId);
        Assert.Single(q2.Items);

        var q3 = Query.ForAllTenants(new[] { item }.AsEnumerable());
        Assert.True(q3.AllTenants);
        Assert.Null(q3.TenantId);
        Assert.Single(q3.Items);
    }

    [Fact]
    public void ForAllTenants_WithEmptyItems_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => Query.ForAllTenants(Array.Empty<QueryItem>()));
        Assert.Throws<ArgumentException>(() => Query.ForAllTenants(Enumerable.Empty<QueryItem>()));
    }
}
