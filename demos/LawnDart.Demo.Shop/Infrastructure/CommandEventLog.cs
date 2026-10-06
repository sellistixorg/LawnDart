using System.Text.Json;
using LawnDart;
using LawnDart.Metadata;

namespace LawnDart.Demo.Shop.Infrastructure;

public sealed record CommandLogEntry(
    Guid CommandId,
    string CommandType,
    string CorrelationId,
    string UserName,
    string UserId,
    DateTime Timestamp,
    bool IsReactorIssued,
    string? ReactorName,
    List<EventLogEntry> Events);

public sealed record EventLogEntry(
    string EventType,
    string StreamId,
    long SequencePosition,
    DateTime Timestamp,
    string EventJson,
    string MetadataJson);

/// <summary>
/// Singleton ring buffer (capacity 200) that stores the most recent commands and their correlated events.
/// The Blazor EventStream page polls this to display a live correlated command/event viewer.
/// </summary>
public sealed class CommandEventLog
{
    private const int Capacity = 200;
    private readonly LinkedList<CommandLogEntry> _entries = new();
    private readonly object _lock = new();

    public event Action? OnChanged;

    public void RecordCommand(
        Guid commandId,
        string commandType,
        string correlationId,
        string userId,
        string userName,
        bool isReactorIssued,
        string? reactorName = null)
    {
        var entry = new CommandLogEntry(
            commandId, commandType, correlationId, userName, userId,
            DateTime.UtcNow, isReactorIssued, reactorName,
            new List<EventLogEntry>());

        lock (_lock)
        {
            _entries.AddFirst(entry);
            while (_entries.Count > Capacity)
                _entries.RemoveLast();
        }

        OnChanged?.Invoke();
    }

    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        WriteIndented          = true,
        PropertyNamingPolicy   = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    public void RecordEvent(
        string correlationId,
        string eventType,
        string streamId,
        long sequencePosition,
        IEvent? eventObj = null,
        EventMetadata? metadata = null)
    {
        var eventJson    = eventObj  is null ? "{}" : JsonSerializer.Serialize(eventObj,  eventObj.GetType(), _jsonOpts);
        var metadataJson = metadata  is null ? "{}" : JsonSerializer.Serialize(metadata,  _jsonOpts);

        lock (_lock)
        {
            var node = _entries.First;
            while (node != null)
            {
                if (node.Value.CorrelationId == correlationId)
                {
                    node.Value.Events.Add(new EventLogEntry(
                        eventType, streamId, sequencePosition, DateTime.UtcNow,
                        eventJson, metadataJson));
                    break;
                }
                node = node.Next;
            }
        }

        OnChanged?.Invoke();
    }

    public IReadOnlyList<CommandLogEntry> GetSnapshot()
    {
        lock (_lock)
            return _entries.ToList();
    }
}
