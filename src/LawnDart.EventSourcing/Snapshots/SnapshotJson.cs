using System.Text.Json;

namespace LawnDart.EventSourcing.Snapshots;

internal static class SnapshotJson
{
    internal static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true
    };
}
