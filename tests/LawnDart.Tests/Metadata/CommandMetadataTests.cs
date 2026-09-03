using LawnDart.Metadata;
using Xunit;

namespace LawnDart.Tests.Metadata;

public class CommandMetadataTests
{
    [Fact]
    public void Constructor_InitializesWithDefaultValues()
    {
        // Act
        var metadata = new CommandMetadata();

        // Assert
        Assert.NotNull(metadata.Custom);
        Assert.True(metadata.Timestamp <= DateTime.UtcNow);
        Assert.True(metadata.Timestamp > DateTime.UtcNow.AddSeconds(-1));
    }

    [Fact]
    public void Properties_CanBeSet()
    {
        // Arrange
        var metadata = new CommandMetadata
        {
            UserId = "user123",
            UserName = "Test User",
            TenantId = "tenant456",
            AccountId = "account789",
            IpAddress = "192.168.1.1",
            UserAgent = "TestAgent/1.0",
            CorrelationId = "corr-123",
            CausationId = "cause-456",
            AuthorizedBy = "admin",
            AuthorizedAt = DateTime.UtcNow,
            AuthorizationPolicies = new[] { "Policy1", "Policy2" },
            Timestamp = DateTime.UtcNow
        };

        // Assert
        Assert.Equal("user123", metadata.UserId);
        Assert.Equal("Test User", metadata.UserName);
        Assert.Equal("tenant456", metadata.TenantId);
        Assert.Equal("account789", metadata.AccountId);
        Assert.Equal("192.168.1.1", metadata.IpAddress);
        Assert.Equal("TestAgent/1.0", metadata.UserAgent);
        Assert.Equal("corr-123", metadata.CorrelationId);
        Assert.Equal("cause-456", metadata.CausationId);
        Assert.Equal("admin", metadata.AuthorizedBy);
        Assert.NotNull(metadata.AuthorizedAt);
        Assert.Equal(2, metadata.AuthorizationPolicies?.Length);
        Assert.Contains("Policy1", metadata.AuthorizationPolicies!);
    }

    [Fact]
    public void Custom_CanStoreAdditionalData()
    {
        // Arrange
        var metadata = new CommandMetadata();

        // Act
        metadata.Custom["Key1"] = "Value1";
        metadata.Custom["Key2"] = "Value2";

        // Assert
        Assert.Equal("Value1", metadata.Custom["Key1"]);
        Assert.Equal("Value2", metadata.Custom["Key2"]);
        Assert.Equal(2, metadata.Custom.Count);
    }
}


