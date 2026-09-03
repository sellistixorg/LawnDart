using LawnDart.Metadata;

namespace LawnDart.TestUtilities;

/// <summary>
/// Test implementation of ITenantContextProvider that provides a fixed tenant ID.
/// </summary>
public class TestTenantContextProvider : ITenantContextProvider
{
    private readonly string? _tenantId;

    public TestTenantContextProvider(string? tenantId = "test-tenant")
    {
        _tenantId = tenantId;
    }

    public string? GetTenantId() => _tenantId;

    public string GetTenantIdRequired() => 
        _tenantId ?? throw new InvalidOperationException("Tenant ID is required but not available");
}

