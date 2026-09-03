namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Describes a discovered lightweight projection, including its logical identity, internal
/// storage identity, projection kind, tenant scope, and optional GET endpoint configuration.
/// </summary>
/// <remarks>
/// Instances are built by <see cref="ProjectionScanner"/> during startup and stored in DI so
/// that <see cref="LawnDart.Projections.Lightweight.Hosting.LightweightProjectionRunnerService"/>
/// and <c>MapProjectionQueries()</c> can share the same discovery results.
/// </remarks>
public sealed class ProjectionRegistration
{
    /// <summary>
    /// Gets the CLR type of the projection handler
    /// (a <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/> subclass).
    /// </summary>
    public Type HandlerType { get; }

    /// <summary>
    /// Gets the CLR type of the view/read model produced by this projection.
    /// Derived from the generic argument of <c>ProjectionBase&lt;TView&gt;</c>.
    /// </summary>
    public Type ViewType { get; }

    /// <summary>
    /// Gets the logical projection family name exposed to callers and metadata
    /// (e.g., <c>"OrderSummary"</c>).
    /// </summary>
    public string LogicalName { get; }

    /// <summary>
    /// Gets the projection implementation version within the logical family.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// Gets the unique internal storage key used for checkpoint and view store keys
    /// (e.g., <c>"OrderSummary:v2"</c>).
    /// </summary>
    public string StorageKey { get; }

    /// <summary>
    /// Gets the display-friendly projection name that includes the version.
    /// </summary>
    public string DisplayName => $"{LogicalName} v{Version}";

    /// <summary>
    /// Compatibility alias for the logical projection family name.
    /// </summary>
    public string ProjectionName => LogicalName;

    /// <summary>
    /// Gets a value indicating whether this version was explicitly marked latest.
    /// </summary>
    public bool DeclaredAsLatest { get; }

    /// <summary>
    /// Gets a value indicating whether this version owns the latest/default alias.
    /// </summary>
    public bool IsLatest { get; }

    /// <summary>
    /// Gets how the latest/default alias was resolved for the logical family.
    /// </summary>
    public LatestVersionResolutionMode LatestResolutionMode { get; }

    /// <summary>
    /// Gets the optional deprecation date as an ISO-8601 string.
    /// </summary>
    public string? DeprecationDateIso { get; }

    /// <summary>
    /// Gets the kind of projection: <see cref="ProjectionKind.SingleStream"/>,
    /// <see cref="ProjectionKind.Global"/>, or <see cref="ProjectionKind.Dcb"/>.
    /// </summary>
    public ProjectionKind Kind { get; }

    /// <summary>
    /// Gets the tenant scope that governs view key construction and GET endpoint protection.
    /// </summary>
    public TenantScope TenantScope { get; }

    /// <summary>
    /// Gets the aggregate stream type for <see cref="ProjectionKind.SingleStream"/> projections
    /// (e.g., <c>"Student"</c>). <see langword="null"/> for other kinds.
    /// </summary>
    public string? StreamType { get; }

    /// <summary>
    /// Gets the DCB query event type names for <see cref="ProjectionKind.Dcb"/> projections.
    /// An empty list means <c>Query.All()</c> is used. Empty for other kinds.
    /// </summary>
    public IReadOnlyList<string> DcbQueryTypes { get; }

    /// <summary>
    /// Gets the optional endpoint configuration. When non-<see langword="null"/>,
    /// <c>MapProjectionQueries()</c> registers a GET endpoint for this projection.
    /// </summary>
    public ProjectionEndpointAttribute? Endpoint { get; }

    /// <summary>
    /// Initializes a new <see cref="ProjectionRegistration"/>.
    /// </summary>
    /// <param name="handlerType">CLR type of the projection handler.</param>
    /// <param name="viewType">CLR type of the view/read model.</param>
    /// <param name="projectionName">Logical name exposed to callers and metadata.</param>
    /// <param name="version">Projection implementation version.</param>
    /// <param name="kind">The projection kind.</param>
    /// <param name="tenantScope">Tenant isolation mode.</param>
    /// <param name="streamType">Aggregate type for single-stream projections.</param>
    /// <param name="dcbQueryTypes">Event type names for DCB projections.</param>
    /// <param name="endpoint">Optional GET endpoint configuration.</param>
    /// <param name="declaredAsLatest">Whether the attribute explicitly marked this version latest.</param>
    /// <param name="isLatest">Whether this version owns the latest/default alias.</param>
    /// <param name="latestResolutionMode">How latest/default alias selection was resolved.</param>
    /// <param name="deprecationDateIso">Optional ISO-8601 deprecation date string.</param>
    public ProjectionRegistration(
        Type handlerType,
        Type viewType,
        string projectionName,
        int version,
        ProjectionKind kind,
        TenantScope tenantScope,
        string? streamType = null,
        IReadOnlyList<string>? dcbQueryTypes = null,
        ProjectionEndpointAttribute? endpoint = null,
        bool declaredAsLatest = false,
        bool isLatest = false,
        LatestVersionResolutionMode latestResolutionMode = LatestVersionResolutionMode.SingleVersionImplicit,
        string? deprecationDateIso = null)
    {
        HandlerType = handlerType ?? throw new ArgumentNullException(nameof(handlerType));
        ViewType = viewType ?? throw new ArgumentNullException(nameof(viewType));
        LogicalName = projectionName ?? throw new ArgumentNullException(nameof(projectionName));
        if (version <= 0)
            throw new ArgumentOutOfRangeException(nameof(version), "Projection version must be greater than zero.");
        Version = version;
        StorageKey = BuildStorageKey(LogicalName, Version);
        Kind = kind;
        TenantScope = tenantScope;
        StreamType = streamType;
        DcbQueryTypes = dcbQueryTypes ?? Array.Empty<string>();
        Endpoint = endpoint;
        DeclaredAsLatest = declaredAsLatest;
        IsLatest = isLatest;
        LatestResolutionMode = latestResolutionMode;
        DeprecationDateIso = deprecationDateIso;
    }

    /// <summary>
    /// Initializes a new <see cref="ProjectionRegistration"/> with projection version <c>1</c>.
    /// </summary>
    public ProjectionRegistration(
        Type handlerType,
        Type viewType,
        string projectionName,
        ProjectionKind kind,
        TenantScope tenantScope,
        string? streamType = null,
        IReadOnlyList<string>? dcbQueryTypes = null,
        ProjectionEndpointAttribute? endpoint = null,
        bool declaredAsLatest = false,
        bool isLatest = false,
        LatestVersionResolutionMode latestResolutionMode = LatestVersionResolutionMode.SingleVersionImplicit,
        string? deprecationDateIso = null)
        : this(
            handlerType: handlerType,
            viewType: viewType,
            projectionName: projectionName,
            version: 1,
            kind: kind,
            tenantScope: tenantScope,
            streamType: streamType,
            dcbQueryTypes: dcbQueryTypes,
            endpoint: endpoint,
            declaredAsLatest: declaredAsLatest,
            isLatest: isLatest,
            latestResolutionMode: latestResolutionMode,
            deprecationDateIso: deprecationDateIso)
    {
    }

    internal ProjectionRegistration WithLatestResolution(
        bool isLatest,
        LatestVersionResolutionMode latestResolutionMode) =>
        new(
            handlerType: HandlerType,
            viewType: ViewType,
            projectionName: LogicalName,
            version: Version,
            kind: Kind,
            tenantScope: TenantScope,
            streamType: StreamType,
            dcbQueryTypes: DcbQueryTypes,
            endpoint: Endpoint,
            declaredAsLatest: DeclaredAsLatest,
            isLatest: isLatest,
            latestResolutionMode: latestResolutionMode,
            deprecationDateIso: DeprecationDateIso);

    private static string BuildStorageKey(string logicalName, int version) => $"{logicalName}:v{version}";
}
