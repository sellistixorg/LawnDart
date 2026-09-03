using System.Text.Json;

namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// Describes the structural difference between two consecutive projection view states.
/// Computed by <see cref="LawnDart.Projections.Lightweight.Debug.ProjectionTimelineService.ComputeDiff"/>.
/// </summary>
/// <remarks>
/// <para>
/// Paths use dot-notation for object members and bracket notation for array elements
/// (e.g. <c>"address.city"</c>, <c>"orders[0].status"</c>, <c>"[1]"</c>).
/// Arrays are walked element-by-element so nested object changes can surface as field-level paths.
/// </para>
/// <para>
/// Path segments use the JSON property names from each payload (camelCase for views serialized
/// with <see cref="ProjectionViewJson.Write"/>).
/// </para>
/// </remarks>
public sealed record ViewStateDiff
{
    /// <summary>Object/member and array-index paths present in <c>next</c> but absent in <c>previous</c>.</summary>
    public required IReadOnlyList<string> AddedPaths { get; init; }

    /// <summary>Object/member and array-index paths present in <c>previous</c> but absent in <c>next</c>.</summary>
    public required IReadOnlyList<string> RemovedPaths { get; init; }

    /// <summary>Properties whose value changed between the two view states.</summary>
    public required IReadOnlyList<ChangedProperty> ChangedProperties { get; init; }

    /// <summary>Returns <see langword="true"/> when there are no additions, removals, or changes.</summary>
    public bool IsEmpty =>
        AddedPaths.Count == 0
        && RemovedPaths.Count == 0
        && ChangedProperties.Count == 0;
}

/// <summary>
/// A single property whose value changed between the previous and next view state.
/// </summary>
/// <param name="Path">Object/member and array-index path of the changed property.</param>
/// <param name="Before">The value in the <em>previous</em> state (null if the property was absent).</param>
/// <param name="After">The value in the <em>next</em> state (null if the property was removed).</param>
public sealed record ChangedProperty(string Path, JsonElement? Before, JsonElement? After);
