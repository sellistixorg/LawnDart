using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.TimeTravelQuery;

namespace LawnDart.Projections.Lightweight.Debug;

internal readonly record struct ProjectionDebugEndpointServices(
    ProjectionTimelineService Timeline,
    IReadOnlyList<ProjectionRegistration> Registrations,
    AdHocProjectionBuilder AdHoc);
