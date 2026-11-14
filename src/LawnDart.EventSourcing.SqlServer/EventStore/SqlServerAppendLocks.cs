using System.Security.Cryptography;
using System.Text;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Named append locks (<c>sp_getapplock</c>) held until the append transaction ends.
/// </summary>
/// <remarks>
/// <para>Every append takes these locks first, before any read or insert:</para>
/// <list type="bullet">
/// <item>The store lock: shared for every append. An append condition takes it
/// exclusive when the condition has no items, or when any item has no tags
/// (types only, or match all).</item>
/// <item>One lock per tag: exclusive for each tag in the append condition, shared
/// for each other tag the append writes.</item>
/// <item>More distinct tags than <see cref="SqlServerEventStoreOptions.AppendLockTagThreshold"/>:
/// the exclusive store lock and no tag locks.</item>
/// </list>
/// <para>The store lock comes first, then tag locks sorted by lock name, so two
/// appends always ask in the same order. Lock names are a short prefix plus a hash
/// of the schema, the events table and the tag. Two names that hash the same only
/// make two appends wait for each other.</para>
    /// <para>The locks, the condition or version check, and the inserts run as one
    /// batch. The condition check is a plain read after the locks in that batch.
    /// Any event it must see was committed before the locks were granted, or its
    /// append is still waiting. Event type names in the condition are read from
    /// the types table in that same read.</para>
/// </remarks>
public partial class SqlServerEventStore
{
    /// <summary>Error raised when an append lock is not granted. Retried like a deadlock.</summary>
    internal const int AppendLockNotGrantedError = 51205;

    internal const string AppendLockPrefix = "LawnDart.append:";

    /// <summary>Version check failed. The message names the expected and current versions.</summary>
    internal const int AppendVersionConflictError = 51206;

    /// <summary>Append condition matched an event. The batch wrote nothing.</summary>
    internal const int AppendConditionConflictError = 51207;

    internal readonly record struct AppendLock(string Resource, bool Exclusive);

    /// <summary>Test seam. Runs after condition type names are looked up and before the locks.</summary>
    internal Func<CancellationToken, Task>? BeforeAppendLocks { get; set; }

    private string AppendLockScope => _schemaName + "\u001f" + _tableName;

    internal IReadOnlyList<AppendLock> AppendLockPlan(
        IReadOnlyList<AppendEvent> envelopes,
        AppendCondition? condition)
        => BuildAppendLockPlan(AppendLockScope, envelopes, condition, _options.AppendLockTagThreshold);

    internal static IReadOnlyList<AppendLock> BuildAppendLockPlan(
        string scope,
        IReadOnlyList<AppendEvent> envelopes,
        AppendCondition? condition,
        int tagThreshold)
    {
        var exclusive = new HashSet<string>(StringComparer.Ordinal);
        if (condition is not null)
        {
            foreach (var item in condition.FailIfEventsMatch.Items)
            {
                if (item.Tags is not { Count: > 0 })
                    continue;
                foreach (var tag in item.Tags)
                {
                    if (!string.IsNullOrEmpty(tag))
                        exclusive.Add(tag);
                }
            }
        }

        var shared = new HashSet<string>(StringComparer.Ordinal);
        foreach (var envelope in envelopes)
        {
            foreach (var tag in envelope.Tags)
            {
                if (!string.IsNullOrEmpty(tag) && !exclusive.Contains(tag))
                    shared.Add(tag);
            }
        }

        var storeLock = StoreLockName(scope);
        if (exclusive.Count + shared.Count > tagThreshold)
            return [new AppendLock(storeLock, Exclusive: true)];

        var storeExclusive = condition is not null && ConditionNeedsStoreLockExclusive(condition);
        var tagLocks = new List<AppendLock>(exclusive.Count + shared.Count);
        foreach (var tag in exclusive)
            tagLocks.Add(new AppendLock(TagLockName(scope, tag), Exclusive: true));
        foreach (var tag in shared)
            tagLocks.Add(new AppendLock(TagLockName(scope, tag), Exclusive: false));

        // Two tags with the same hash share one lock. Keep the stronger mode.
        var byName = new Dictionary<string, bool>(StringComparer.Ordinal);
        foreach (var tagLock in tagLocks)
            byName[tagLock.Resource] = byName.TryGetValue(tagLock.Resource, out var held) ? held || tagLock.Exclusive : tagLock.Exclusive;

        var plan = new List<AppendLock>(byName.Count + 1) { new(storeLock, storeExclusive) };
        foreach (var name in byName.Keys.OrderBy(static n => n, StringComparer.Ordinal))
            plan.Add(new AppendLock(name, byName[name]));
        return plan;
    }

    /// <summary>
    /// True when the condition has no items, or an item without tags.
    /// Tag locks cannot cover such an item, so the store lock is taken exclusive.
    /// </summary>
    internal static bool ConditionNeedsStoreLockExclusive(AppendCondition condition)
    {
        if (condition.FailIfEventsMatch.Items.Count == 0)
            return true;

        foreach (var item in condition.FailIfEventsMatch.Items)
        {
            if (item.Tags is not { Count: > 0 } || item.Tags.All(string.IsNullOrEmpty))
                return true;
        }

        return false;
    }

    internal static string StoreLockName(string scope)
        => AppendLockPrefix + Hash(scope) + ":store";

    internal static string TagLockName(string scope, string tag)
        => AppendLockPrefix + Hash(scope) + ":tag:" + Hash(scope + "\u001f" + tag);

    private static string Hash(string text)
    {
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(text), digest);
        return Convert.ToHexString(digest[..16]);
    }

}
