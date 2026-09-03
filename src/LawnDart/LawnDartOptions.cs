namespace LawnDart;

/// <summary>
/// Cross-cutting configuration options for LawnDart.
/// Store-specific options live in their respective packages:
/// <c>SqlServerEventStoreOptions</c> (LawnDart.EventSourcing.SqlServer) for SQL Server event store,
/// <c>EventSourcingOptions</c> (LawnDart.EventSourcing) for in-process event sourcing.
/// </summary>
public class LawnDartOptions
{
    /// <summary>
    /// Azure Storage connection string.
    /// </summary>
    public string? AzureStorageConnection { get; set; }

    /// <summary>
    /// Require tenant ID in all framework operations.
    /// When true, TenantId must be present in CommandMetadata.
    /// When false, tenant is optional.
    /// Default: true
    /// </summary>
    public bool RequireTenantId { get; set; } = true;

    /// <summary>
    /// Enable authorization checks on command execution.
    /// When true, commands decorated with authorization attributes will be checked before execution.
    /// Requires AuthorizationService to be registered.
    /// Default: false
    /// </summary>
    public bool EnableAuthorization { get; set; } = false;
}
