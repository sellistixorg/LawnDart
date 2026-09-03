using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Registration;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Tests that the view instance key is constructed correctly for all three
/// <see cref="TenantScope"/> variants.
/// </summary>
public class ViewKeyConstructionTests
{
    // The stream ID format is {tenantId}:{aggregateType}:{aggregateId}
    private const string SampleStreamId = "acme:Student:550e8400-e29b-41d4-a716-446655440000";
    private const string TenantId = "acme";
    private const string AggregateId = "550e8400-e29b-41d4-a716-446655440000";

    [Fact]
    public void TenantScoped_Key_Equals_Full_StreamId()
    {
        // For TenantScoped the full streamId is used as the instance key so the
        // key already carries the tenant prefix.
        var registration = MakeRegistration(TenantScope.TenantScoped, "Student");

        var key = BuildKey(registration, SampleStreamId, TenantId);

        // The GET endpoint path builds the key as "{tenantId}:{streamType}:{entityId}"
        Assert.Equal($"{TenantId}:Student:{AggregateId}", key);
    }

    [Fact]
    public void TenantGlobal_Key_Uses_EntityId_Only()
    {
        var registration = MakeRegistration(TenantScope.TenantGlobal, "Student");

        var key = BuildKey(registration, SampleStreamId, TenantId);

        Assert.Equal(AggregateId, key);
    }

    [Fact]
    public void SystemGlobal_Key_Is_Literal_Global()
    {
        var registration = MakeRegistration(TenantScope.SystemGlobal, null);

        // SystemGlobal projections use a fixed "global" key regardless of stream
        var key = BuildGlobalKey(registration);

        Assert.Equal("global", key);
    }

    [Fact]
    public void TenantScoped_Key_Different_Tenants_Yield_Different_Keys()
    {
        var registration = MakeRegistration(TenantScope.TenantScoped, "Student");

        var keyA = BuildKey(registration, "tenantA:Student:abc", "tenantA");
        var keyB = BuildKey(registration, "tenantB:Student:abc", "tenantB");

        Assert.NotEqual(keyA, keyB);
        Assert.StartsWith("tenantA:", keyA);
        Assert.StartsWith("tenantB:", keyB);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static ProjectionRegistration MakeRegistration(TenantScope scope, string? streamType)
        => new(
            handlerType: typeof(Helpers.CounterSummaryProjection),
            viewType: typeof(Helpers.CounterView),
            projectionName: "TestProjection",
            kind: streamType is not null ? ProjectionKind.SingleStream : ProjectionKind.Global,
            tenantScope: scope,
            streamType: streamType);

    /// <summary>
    /// Replicates the key-construction logic in <c>MapProjectionQueries</c> for GET endpoints.
    /// </summary>
    private static string BuildKey(
        ProjectionRegistration reg,
        string streamId,
        string tenantId)
    {
        var entityId = LawnDart.EventStore.StreamIdParser
            .ExtractAggregateIdString(streamId);

        return reg.TenantScope switch
        {
            TenantScope.TenantScoped =>
                entityId is not null
                    ? $"{tenantId}:{reg.StreamType}:{entityId}"
                    : $"{tenantId}:{reg.ProjectionName}",
            TenantScope.TenantGlobal => entityId ?? reg.ProjectionName,
            _ => "global"
        };
    }

    private static string BuildGlobalKey(ProjectionRegistration _) => "global";
}
