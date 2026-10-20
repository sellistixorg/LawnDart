using System.Text.Json;
using LawnDart;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// Tenant checks shared by the in-memory store and the SQL Server store.
/// </summary>
internal static class AppendTenantRules
{
    public static void ValidateStreamAppend(
        string streamId,
        IReadOnlyList<AppendEvent> envelopes,
        bool requireTenantId,
        bool validateNames)
    {
        var tenantIdFromStream = StreamIdParser.ExtractTenantId(streamId);
        if (requireTenantId && tenantIdFromStream == null)
        {
            throw new ArgumentException(
                $"Stream ID '{streamId}' must include tenant ID in format '{{tenantId}}:{{aggregateType}}:{{aggregateId}}' " +
                $"when RequireTenantId is true.",
                nameof(streamId));
        }

        if (requireTenantId && tenantIdFromStream != null)
            LawnDartNames.ValidateTenantId(tenantIdFromStream, nameof(streamId), validateNames);

        var tenantIdFromMetadata = TryReadTenantId(envelopes);
        if (requireTenantId && tenantIdFromMetadata != null)
            LawnDartNames.ValidateTenantId(tenantIdFromMetadata, "metadata", validateNames);

        if (tenantIdFromStream != null && tenantIdFromMetadata != null && tenantIdFromMetadata != tenantIdFromStream)
        {
            throw new ArgumentException(
                $"Tenant ID mismatch: stream ID has '{tenantIdFromStream}' but metadata has '{tenantIdFromMetadata}'.");
        }
    }

    public static void ValidateDcbAppend(string? tenantId, bool requireTenantId, bool validateNames)
    {
        if (requireTenantId && string.IsNullOrWhiteSpace(tenantId))
        {
            throw new ArgumentException(
                "TenantId is required in event metadata when RequireTenantId is true for DCB appends.");
        }

        if (requireTenantId && !string.IsNullOrWhiteSpace(tenantId))
            LawnDartNames.ValidateTenantId(tenantId, "metadata", validateNames);
    }

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
