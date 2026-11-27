using System.Text.Json;
using LawnDart;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// Tenant checks shared by the in-memory store, the SQL Server store, and the Postgres store.
/// </summary>
internal static class AppendTenantRules
{
    public static void ValidateStreamAppend(
        StreamId streamId,
        IReadOnlyList<AppendEvent> envelopes,
        Tenancy tenancy,
        string? singleTenantId,
        bool validateNames)
    {
        var tenantIdFromStream = streamId.TenantId;
        var tenantIdFromMetadata = TryReadTenantId(envelopes);

        if (tenancy == Tenancy.Shared)
        {
            if (tenantIdFromStream == null)
            {
                throw new ArgumentException(
                    $"Stream ID '{streamId}' must include tenant ID in format '{{tenantId}}:{{aggregateType}}:{{aggregateId}}' " +
                    "when Tenancy is Shared.",
                    nameof(streamId));
            }

            LawnDartNames.ValidateTenantId(tenantIdFromStream, nameof(streamId), validateNames);

            if (tenantIdFromMetadata != null)
            {
                LawnDartNames.ValidateTenantId(tenantIdFromMetadata, "metadata", validateNames);
                if (!string.Equals(tenantIdFromMetadata, tenantIdFromStream, StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        $"Tenant ID mismatch: stream ID has '{tenantIdFromStream}' but metadata has '{tenantIdFromMetadata}'.");
                }
            }

            return;
        }

        if (singleTenantId is null)
        {
            if (tenantIdFromStream != null)
            {
                throw new ArgumentException(
                    $"Stream ID '{streamId}' includes tenant prefix '{tenantIdFromStream}', which is not allowed when Tenancy is Single without SingleTenantId.",
                    nameof(streamId));
            }

            if (tenantIdFromMetadata != null)
            {
                throw new ArgumentException(
                    $"Tenant ID '{tenantIdFromMetadata}' in metadata is not allowed when Tenancy is Single without SingleTenantId.",
                    nameof(envelopes));
            }

            return;
        }

        if (tenantIdFromStream != null && !string.Equals(tenantIdFromStream, singleTenantId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Stream ID '{streamId}' has tenant prefix '{tenantIdFromStream}', which does not match SingleTenantId '{singleTenantId}'.",
                nameof(streamId));
        }

        if (tenantIdFromMetadata != null && !string.Equals(tenantIdFromMetadata, singleTenantId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tenant ID '{tenantIdFromMetadata}' in metadata does not match SingleTenantId '{singleTenantId}'.",
                nameof(envelopes));
        }
    }

    public static (string? StorageTenant, string? ConditionTenant) ValidateDcbAppend(
        string? metadataTenantId,
        AppendCondition condition,
        Tenancy tenancy,
        string? singleTenantId,
        bool validateNames)
    {
        ArgumentNullException.ThrowIfNull(condition);
        var condQuery = condition.FailIfEventsMatch;
        if (condQuery.AllTenants && condQuery.TenantId is not null)
        {
            throw new ArgumentException(
                "Append condition query cannot specify both TenantId and AllTenants.",
                nameof(condition));
        }

        if (tenancy == Tenancy.Shared)
        {
            if (string.IsNullOrWhiteSpace(metadataTenantId))
            {
                throw new ArgumentException(
                    "TenantId is required in event metadata when Tenancy is Shared for DCB appends.");
            }

            LawnDartNames.ValidateTenantId(metadataTenantId, "metadata", validateNames);

            if (!condQuery.AllTenants && condQuery.TenantId is { } condTenant)
            {
                LawnDartNames.ValidateTenantId(condTenant, nameof(condition), validateNames);
                if (!string.Equals(condTenant, metadataTenantId, StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        $"Append condition query tenant '{condTenant}' does not match append tenant '{metadataTenantId}'.",
                        nameof(condition));
                }
            }

            return (metadataTenantId, condQuery.AllTenants ? null : metadataTenantId);
        }

        if (singleTenantId is null)
        {
            if (!string.IsNullOrWhiteSpace(metadataTenantId))
            {
                throw new ArgumentException(
                    $"TenantId '{metadataTenantId}' in event metadata is not allowed when Tenancy is Single without SingleTenantId.");
            }

            if (condQuery.TenantId is not null)
            {
                throw new ArgumentException(
                    $"Append condition query TenantId '{condQuery.TenantId}' is not allowed when Tenancy is Single without SingleTenantId.",
                    nameof(condition));
            }

            return (null, null);
        }

        if (!string.IsNullOrWhiteSpace(metadataTenantId)
            && !string.Equals(metadataTenantId, singleTenantId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"TenantId '{metadataTenantId}' in event metadata does not match SingleTenantId '{singleTenantId}'.");
        }

        if (condQuery.TenantId is { } singleCondTenant
            && !string.Equals(singleCondTenant, singleTenantId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Append condition query TenantId '{singleCondTenant}' does not match SingleTenantId '{singleTenantId}'.",
                nameof(condition));
        }

        return (singleTenantId, null);
    }

    public static string? ValidateQuery(
        Query query,
        Tenancy tenancy,
        string? singleTenantId,
        bool validateNames)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (query.AllTenants && query.TenantId is not null)
        {
            throw new ArgumentException(
                "Query cannot specify both TenantId and AllTenants.",
                nameof(query));
        }

        if (tenancy == Tenancy.Shared)
        {
            if (query.AllTenants)
                return null;

            if (string.IsNullOrWhiteSpace(query.TenantId))
            {
                throw new ArgumentException(
                    "In Tenancy.Shared mode, Query must set TenantId via ForTenant(...) or be built with Query.ForAllTenants().",
                    nameof(query));
            }

            LawnDartNames.ValidateTenantId(query.TenantId, nameof(query), validateNames);
            return query.TenantId;
        }

        if (singleTenantId is null)
        {
            if (query.TenantId is not null)
            {
                throw new ArgumentException(
                    $"Query.TenantId '{query.TenantId}' is not allowed when Tenancy is Single without SingleTenantId.",
                    nameof(query));
            }

            return null;
        }

        if (query.TenantId is not null && !string.Equals(query.TenantId, singleTenantId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Query.TenantId '{query.TenantId}' does not match SingleTenantId '{singleTenantId}'.",
                nameof(query));
        }

        return null;
    }

    public static string? ResolveStreamStorageTenant(StreamId streamId, Tenancy tenancy, string? singleTenantId)
        => tenancy == Tenancy.Single && singleTenantId is not null
            ? singleTenantId
            : streamId.TenantId;

    public static string? ResolveStreamStorageTenant(string streamId, Tenancy tenancy, string? singleTenantId)
        => tenancy == Tenancy.Single && singleTenantId is not null
            ? singleTenantId
            : StreamIdText.TenantId(streamId);

    public static string? TryReadTenantId(IReadOnlyList<AppendEvent> envelopes)
    {
        foreach (var envelope in envelopes)
        {
            if (envelope.Metadata.IsEmpty)
                continue;

            try
            {
                using var doc = JsonDocument.Parse(envelope.Metadata);
                if (doc.RootElement.TryGetProperty("TenantId", out var property)
                    && property.ValueKind == JsonValueKind.String)
                {
                    var value = property.GetString();
                    if (!string.IsNullOrWhiteSpace(value))
                        return value;
                }
            }
            catch (JsonException)
            {
                // Metadata is opaque JSON to the log; skip unreadable blobs.
            }
        }

        return null;
    }
}
