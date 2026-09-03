using LawnDart;
using LawnDart.Metadata;
using Xunit;

namespace LawnDart.Tests.Metadata;

/// <summary>
/// Tests for tenant-aware metadata provider functionality.
/// </summary>
public class TenantAwareMetadataProviderTests
{
    [Fact]
    public void CaptureCommandMetadata_WithTenantProvider_AutoPopulatesTenantId()
    {
        // Arrange
        var tenantProvider = new TestTenantContextProvider("tenant-123");
        var metadataProvider = new DefaultMetadataProvider(tenantProvider);

        // Act
        var metadata = metadataProvider.CaptureCommandMetadata();

        // Assert
        Assert.NotNull(metadata);
        Assert.Equal("tenant-123", metadata.TenantId);
        Assert.NotNull(metadata.CorrelationId);
        Assert.True(metadata.Timestamp <= DateTime.UtcNow);
    }

    [Fact]
    public void CaptureCommandMetadata_WithoutTenantProvider_TenantIdIsNull()
    {
        // Arrange
        var metadataProvider = new DefaultMetadataProvider();

        // Act
        var metadata = metadataProvider.CaptureCommandMetadata();

        // Assert
        Assert.NotNull(metadata);
        Assert.Null(metadata.TenantId);
        Assert.NotNull(metadata.CorrelationId);
    }

    [Fact]
    public void CaptureCommandMetadata_WithNullTenantProvider_TenantIdIsNull()
    {
        // Arrange
        var metadataProvider = new DefaultMetadataProvider(null);

        // Act
        var metadata = metadataProvider.CaptureCommandMetadata();

        // Assert
        Assert.NotNull(metadata);
        Assert.Null(metadata.TenantId);
    }

    [Fact]
    public void CaptureCommandMetadata_WithTenantProviderReturningNull_TenantIdIsNull()
    {
        // Arrange
        var tenantProvider = new TestTenantContextProvider(null);
        var metadataProvider = new DefaultMetadataProvider(tenantProvider);

        // Act
        var metadata = metadataProvider.CaptureCommandMetadata();

        // Assert
        Assert.NotNull(metadata);
        Assert.Null(metadata.TenantId);
    }

    [Fact]
    public void CaptureCommandMetadata_MultipleCalls_GeneratesDifferentCorrelationIds()
    {
        // Arrange
        var tenantProvider = new TestTenantContextProvider("tenant-123");
        var metadataProvider = new DefaultMetadataProvider(tenantProvider);

        // Act
        var metadata1 = metadataProvider.CaptureCommandMetadata();
        var metadata2 = metadataProvider.CaptureCommandMetadata();

        // Assert
        Assert.NotEqual(metadata1.CorrelationId, metadata2.CorrelationId);
        Assert.Equal("tenant-123", metadata1.TenantId);
        Assert.Equal("tenant-123", metadata2.TenantId);
    }

    private class TestTenantContextProvider : ITenantContextProvider
    {
        private readonly string? _tenantId;

        public TestTenantContextProvider(string? tenantId)
        {
            _tenantId = tenantId;
        }

        public string? GetTenantId() => _tenantId;

        public string GetTenantIdRequired() =>
            _tenantId ?? throw new InvalidOperationException("Tenant ID is required but not available");
    }
}
