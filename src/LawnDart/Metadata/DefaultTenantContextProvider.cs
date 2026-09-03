namespace LawnDart.Metadata;

/// <summary>
/// Default tenant context provider that extracts tenant ID from metadata provider.
/// </summary>
public class DefaultTenantContextProvider : ITenantContextProvider
{
    private readonly IMetadataProvider _metadataProvider;

    public DefaultTenantContextProvider(IMetadataProvider metadataProvider)
    {
        _metadataProvider = metadataProvider ?? throw new ArgumentNullException(nameof(metadataProvider));
    }

    /// <inheritdoc />
    public string? GetTenantId()
    {
        var metadata = _metadataProvider.CaptureCommandMetadata();
        return metadata.TenantId;
    }

    /// <inheritdoc />
    public string GetTenantIdRequired()
    {
        var tenantId = GetTenantId();
        if (string.IsNullOrWhiteSpace(tenantId))
        {
            throw new InvalidOperationException(
                "Tenant ID is required but not available in the current context. " +
                "Ensure tenant ID is set in command metadata or provide a custom ITenantContextProvider.");
        }
        return tenantId;
    }
}

