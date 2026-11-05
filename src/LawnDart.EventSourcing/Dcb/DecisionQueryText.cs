using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Dcb;

/// <summary>
/// One readable string for the operator column beside a decision-query snapshot.
/// The column is not the snapshot key.
/// </summary>
internal static class DecisionQueryText
{
    public static string Format(Query query)
    {
        var parts = new string[query.Items.Count];
        for (var i = 0; i < query.Items.Count; i++)
        {
            var item = query.Items[i];
            var bits = new List<string>(2);
            if (item.Types is { Count: > 0 })
                bits.Add("types=" + string.Join(",", item.Types));
            if (item.Tags is { Count: > 0 })
                bits.Add("tags=" + string.Join(",", item.Tags));
            parts[i] = "[" + string.Join("; ", bits) + "]";
        }

        return string.Join(" | ", parts);
    }
}
