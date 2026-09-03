using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionInstanceIdsTests
{
    [Theory]
    [InlineData(0, 1, "global")]
    [InlineData(0, 0, "global")]
    [InlineData(5, 1, "global")]
    public void ForUnpartitioned_SingleNode_ReturnsGlobal(int node, int total, string expected)
        => Assert.Equal(expected, ProjectionInstanceIds.ForUnpartitioned(node, total));

    [Theory]
    [InlineData(0, 3, "global:n0")]
    [InlineData(1, 3, "global:n1")]
    [InlineData(2, 3, "global:n2")]
    public void ForUnpartitioned_MultiNode_ReturnsNodeSuffix(int node, int total, string expected)
        => Assert.Equal(expected, ProjectionInstanceIds.ForUnpartitioned(node, total));

    [Fact]
    public void ForUnpartitioned_InvalidNode_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() =>
            ProjectionInstanceIds.ForUnpartitioned(3, 3));

    [Fact]
    public void ForUnpartitioned_FromPartitioningService_UsesNodeSettings()
    {
        var partitioning = new ConsistentHashPartitioningService(1, 2);
        Assert.Equal("global:n1", ProjectionInstanceIds.ForUnpartitioned(partitioning));
    }

    [Fact]
    public void ForUnpartitioned_FromOptions_UsesNodeSettings()
    {
        var options = new LightweightProjectionOptions { NodeInstance = 0, TotalInstances = 2 };
        Assert.Equal("global:n0", ProjectionInstanceIds.ForUnpartitioned(options));
    }

    [Theory]
    [InlineData(ProjectionKind.Global, true)]
    [InlineData(ProjectionKind.Dcb, true)]
    [InlineData(ProjectionKind.SingleStream, false)]
    [InlineData(ProjectionKind.MultiStream, false)]
    public void IsUnpartitionedKind_MatchesGlobalAndDcb(ProjectionKind kind, bool expected)
        => Assert.Equal(expected, ProjectionInstanceIds.IsUnpartitionedKind(kind));
}
