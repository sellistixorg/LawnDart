using System.Globalization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Shared helpers for projection GET freshness headers and <c>minSequence</c> checks.
/// </summary>
internal static class ProjectionFreshness
{
    public const string SequenceHeader = "X-Projection-Sequence";
    public const string LagMsHeader = "X-Projection-Lag-Ms";
    public const string ReadSourceHeader = "X-Projection-Read-Source";
    public const string NotCaughtUpCode = "projection_not_caught_up";

    public const string ReadSourceMemory = "memory";
    public const string ReadSourceSql = "sql";

    /// <summary>
    /// Infers a coarse read-source label from the concrete <see cref="IViewStore"/> type.
    /// </summary>
    public static string InferReadSource(IViewStore viewStore) =>
        viewStore switch
        {
            InMemoryViewStore => ReadSourceMemory,
            SqlViewStore => ReadSourceSql,
            _ => ReadSourceSql
        };

    public static bool TryParseMinSequence(HttpRequest request, out long minSequence)
    {
        minSequence = 0;
        if (!request.Query.TryGetValue("minSequence", out var values))
            return false;

        var raw = values.FirstOrDefault();
        return !string.IsNullOrWhiteSpace(raw)
               && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out minSequence);
    }

    public static void WriteSuccessHeaders(
        HttpResponse response,
        long sequence,
        string readSource,
        double? lagMs = null)
    {
        response.Headers[SequenceHeader] = sequence.ToString(CultureInfo.InvariantCulture);
        response.Headers[ReadSourceHeader] = readSource;

        if (lagMs is not null)
        {
            response.Headers[LagMsHeader] = Math.Max(0, lagMs.Value)
                .ToString("0", CultureInfo.InvariantCulture);
        }
    }

    public static IResult NotCaughtUp(long viewSequence, long minSequence) =>
        Results.Problem(new ProblemDetails
        {
            Status = StatusCodes.Status409Conflict,
            Title = "Projection not caught up",
            Detail =
                $"View sequence {viewSequence.ToString(CultureInfo.InvariantCulture)} is behind " +
                $"requested minSequence {minSequence.ToString(CultureInfo.InvariantCulture)}.",
            Extensions =
            {
                ["code"] = NotCaughtUpCode,
                ["viewSequence"] = viewSequence,
                ["minSequence"] = minSequence
            }
        });
}
