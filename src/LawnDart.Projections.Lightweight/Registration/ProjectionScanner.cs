using System.Reflection;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Projections.Sdk;

namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Scans one or more assemblies for types decorated with <see cref="SingleStreamProjectionAttribute"/>,
/// <see cref="GlobalProjectionAttribute"/>, <see cref="DcbProjectionAttribute"/>, or
/// <see cref="MultiStreamProjectionAttribute"/> and produces a list of
/// <see cref="ProjectionRegistration"/> instances used by the runner and endpoint mapper.
/// </summary>
public static class ProjectionScanner
{
    /// <summary>
    /// Scans the provided assemblies and returns all discovered projection registrations.
    /// </summary>
    /// <param name="assemblies">One or more assemblies to scan for projection handler types.</param>
    /// <returns>
    /// A read-only list of <see cref="ProjectionRegistration"/> instances, one per discovered
    /// projection class.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a decorated type does not inherit from
    /// <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/>.
    /// </exception>
    public static IReadOnlyList<ProjectionRegistration> Scan(params Assembly[] assemblies)
    {
        var results = new List<ProjectionRegistration>();

        foreach (var assembly in assemblies)
        {
            foreach (var type in assembly.GetExportedTypes())
            {
                if (!type.IsClass || type.IsAbstract)
                    continue;

                var registration = TryBuildRegistration(type);
                if (registration is not null)
                    results.Add(registration);
            }
        }

        return FinalizeRegistrations(results);
    }

    /// <summary>
    /// Attempts to build a <see cref="ProjectionRegistration"/> from a single type.
    /// Returns <see langword="null"/> when the type carries none of the expected attributes.
    /// </summary>
    /// <param name="type">The CLR type to inspect.</param>
    /// <returns>
    /// A <see cref="ProjectionRegistration"/>, or <see langword="null"/> if the type is
    /// not a lightweight projection.
    /// </returns>
    public static ProjectionRegistration? TryBuildRegistration(Type type)
    {
        var endpoint = type.GetCustomAttribute<ProjectionEndpointAttribute>();

        if (type.GetCustomAttribute<SingleStreamProjectionAttribute>() is { } ssp)
        {
            var viewType = ExtractViewType(type)
                ?? throw new InvalidOperationException(
                    $"Type '{type.FullName}' is decorated with [SingleStreamProjection] but does not " +
                    $"inherit from ProjectionBase<TView>.");

            return new ProjectionRegistration(
                handlerType: type,
                viewType: viewType,
                projectionName: ssp.LogicalName,
                version: ssp.Version,
                kind: ProjectionKind.SingleStream,
                tenantScope: ssp.TenantScope,
                streamType: ssp.StreamType,
                endpoint: endpoint,
                declaredAsLatest: ssp.IsLatest,
                deprecationDateIso: ssp.DeprecationDateIso);
        }

        if (type.GetCustomAttribute<GlobalProjectionAttribute>() is { } gp)
        {
            var viewType = ExtractViewType(type)
                ?? throw new InvalidOperationException(
                    $"Type '{type.FullName}' is decorated with [GlobalProjection] but does not " +
                    $"inherit from ProjectionBase<TView>.");

            return new ProjectionRegistration(
                handlerType: type,
                viewType: viewType,
                projectionName: gp.LogicalName,
                version: gp.Version,
                kind: ProjectionKind.Global,
                tenantScope: gp.TenantScope,
                endpoint: endpoint,
                declaredAsLatest: gp.IsLatest,
                deprecationDateIso: gp.DeprecationDateIso);
        }

        if (type.GetCustomAttribute<DcbProjectionAttribute>() is { } dcb)
        {
            var viewType = ExtractViewType(type)
                ?? throw new InvalidOperationException(
                    $"Type '{type.FullName}' is decorated with [DcbProjection] but does not " +
                    $"inherit from ProjectionBase<TView>.");

            return new ProjectionRegistration(
                handlerType: type,
                viewType: viewType,
                projectionName: dcb.LogicalName,
                version: dcb.Version,
                kind: ProjectionKind.Dcb,
                tenantScope: dcb.TenantScope,
                dcbQueryTypes: NormalizeDcbQueryTypeNames(type.Assembly, dcb.QueryTypes),
                endpoint: endpoint,
                declaredAsLatest: dcb.IsLatest,
                deprecationDateIso: dcb.DeprecationDateIso);
        }

        if (type.GetCustomAttribute<MultiStreamProjectionAttribute>() is { } msp)
        {
            var viewType = ExtractViewType(type)
                ?? throw new InvalidOperationException(
                    $"Type '{type.FullName}' is decorated with [MultiStreamProjection] but does not " +
                    $"inherit from ProjectionBase<TView>.");

            if (!typeof(IMultiStreamEntityResolver).IsAssignableFrom(type))
                throw new InvalidOperationException(
                    $"Type '{type.FullName}' is decorated with [MultiStreamProjection] but does not " +
                    $"implement IMultiStreamEntityResolver. Add 'string? GetEntityId(IEvent e)' to " +
                    $"the class.");

            var queryTypes = msp.EventTypes.Length > 0
                ? (IReadOnlyList<string>)msp.EventTypes
                    .Select(t => LawnDart.EventStore.EventTypeNameResolver.GetName(t))
                    .ToArray()
                : Array.Empty<string>();

            return new ProjectionRegistration(
                handlerType: type,
                viewType: viewType,
                projectionName: msp.LogicalName,
                version: msp.Version,
                kind: ProjectionKind.MultiStream,
                tenantScope: msp.TenantScope,
                dcbQueryTypes: queryTypes,
                endpoint: endpoint,
                declaredAsLatest: msp.IsLatest,
                deprecationDateIso: msp.DeprecationDateIso);
        }

        return null;
    }

    private static IReadOnlyList<ProjectionRegistration> FinalizeRegistrations(
        IReadOnlyList<ProjectionRegistration> registrations)
    {
        if (registrations.Count == 0)
            return registrations;

        var latestByStorageKey = new Dictionary<string, ProjectionRegistration>(StringComparer.Ordinal);

        foreach (var family in registrations.GroupBy(r => r.LogicalName, StringComparer.OrdinalIgnoreCase))
        {
            var duplicateVersions = family
                .GroupBy(r => r.Version)
                .Where(g => g.Count() > 1)
                .ToList();

            if (duplicateVersions.Count > 0)
            {
                var duplicateList = string.Join(
                    ", ",
                    duplicateVersions.Select(g => $"v{g.Key}"));
                throw new InvalidOperationException(
                    $"Logical projection '{family.Key}' has duplicate version registrations: {duplicateList}. " +
                    "Each (LogicalName, Version) pair must be unique.");
            }

            var explicitLatest = family.Where(r => r.DeclaredAsLatest).ToList();
            if (explicitLatest.Count > 1)
            {
                var latestList = string.Join(", ", explicitLatest.Select(r => $"v{r.Version}"));
                throw new InvalidOperationException(
                    $"Logical projection '{family.Key}' has multiple versions marked latest: {latestList}. " +
                    "Mark at most one version with IsLatest = true.");
            }

            ProjectionRegistration latest;
            LatestVersionResolutionMode resolutionMode;

            if (explicitLatest.Count == 1)
            {
                latest = explicitLatest[0];
                resolutionMode = LatestVersionResolutionMode.Explicit;
            }
            else if (family.Count() == 1)
            {
                latest = family.Single();
                resolutionMode = LatestVersionResolutionMode.SingleVersionImplicit;
            }
            else
            {
                latest = family
                    .OrderByDescending(r => r.Version)
                    .ThenBy(r => r.HandlerType.FullName, StringComparer.Ordinal)
                    .First();
                resolutionMode = LatestVersionResolutionMode.HighestVersionFallback;
            }

            foreach (var registration in family)
            {
                var resolved = registration.WithLatestResolution(
                    isLatest: registration.Version == latest.Version,
                    latestResolutionMode: resolutionMode);
                latestByStorageKey[registration.StorageKey] = resolved;
            }
        }

        return registrations
            .Select(r => latestByStorageKey[r.StorageKey])
            .ToList();
    }

    /// <summary>
    /// Maps <see cref="DcbProjectionAttribute"/> string tokens to stable names understood by
    /// <see cref="EventTypeNameResolver"/> and <see cref="IEventStore.ReadByQueryStreamAsync"/> filters.
    /// </summary>
    /// <remarks>
    /// Attributes often use short CLR type names (e.g. <c>"OrderPlaced"</c>).  The in-memory and
    /// persisted event stores match on <see cref="EventTypeNameResolver.GetName(Type)"/> (typically
    /// namespace-qualified).  When a token contains no <c>'.'</c>, resolve it against the handler
    /// assembly's exported <see cref="IEvent"/> types by simple name; if ambiguous, fail fast.
    /// </remarks>
    private static IReadOnlyList<string> NormalizeDcbQueryTypeNames(Assembly assembly, string[] queryTokens)
    {
        if (queryTokens.Length == 0)
            return Array.Empty<string>();

        return queryTokens.Select(t => NormalizeDcbQueryTypeName(assembly, t)).ToArray();
    }

    private static string NormalizeDcbQueryTypeName(Assembly assembly, string queryToken)
    {
        if (string.IsNullOrWhiteSpace(queryToken))
            throw new InvalidOperationException("DcbProjection query type token cannot be empty.");

        if (queryToken.Contains('.', StringComparison.Ordinal))
            return queryToken;

        Type[] matches;
        try
        {
            matches = assembly.GetExportedTypes()
                .Where(t => t is { IsClass: true, IsAbstract: false } && typeof(IEvent).IsAssignableFrom(t)
                    && string.Equals(t.Name, queryToken, StringComparison.Ordinal))
                .ToArray();
        }
        catch (ReflectionTypeLoadException ex)
        {
            throw new InvalidOperationException(
                $"Failed to resolve DcbProjection query token '{queryToken}' in assembly '{assembly.FullName}'.", ex);
        }

        if (matches.Length == 1)
            return EventTypeNameResolver.GetName(matches[0]);

        if (matches.Length > 1)
        {
            var names = string.Join(
                ", ",
                matches.Select(m => m.FullName).OrderBy(n => n, StringComparer.Ordinal));
            throw new InvalidOperationException(
                $"DcbProjection query token '{queryToken}' is ambiguous in assembly '{assembly.FullName}'. " +
                $"Matches: {names}.");
        }

        return queryToken;
    }

    /// <summary>
    /// Walks the type hierarchy to find the closed <c>ProjectionBase&lt;TView&gt;</c> base class
    /// and returns the <c>TView</c> type argument.
    /// </summary>
    /// <param name="type">The projection handler type.</param>
    /// <returns>The view type, or <see langword="null"/> if not found.</returns>
    public static Type? ExtractViewType(Type type)
    {
        var current = type.BaseType;
        while (current != null && current != typeof(object))
        {
            if (current.IsGenericType &&
                current.GetGenericTypeDefinition() == typeof(ProjectionBase<>))
            {
                return current.GetGenericArguments()[0];
            }
            current = current.BaseType;
        }
        return null;
    }
}
