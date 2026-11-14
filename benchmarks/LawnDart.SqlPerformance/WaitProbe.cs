using Microsoft.Data.SqlClient;

namespace LawnDart.SqlPerformance;

/// <summary>
/// Optional wait sample for a local run. It stays quiet unless LAWNDART_SQLPERF_WAITS is 1.
/// The text file is under the gitignored results folder.
/// </summary>
internal static class WaitProbe
{
    public static async Task AroundAsync(
        string connectionString,
        Func<Task> body,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_WAITS"), "1", StringComparison.Ordinal))
        {
            await body().ConfigureAwait(false);
            return;
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var before = await ReadWaitsAsync(connection, cancellationToken).ConfigureAwait(false);
        var hits = new Dictionary<string, int>(StringComparer.Ordinal);
        using var stop = new CancellationTokenSource();
        var poll = PollAsync(connectionString, hits, stop.Token);
        try
        {
            await body().ConfigureAwait(false);
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            try
            {
                await poll.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The poll loop ended with the appends.
            }
        }

        var after = await ReadWaitsAsync(connection, CancellationToken.None).ConfigureAwait(false);
        var directory = Reports.ResultsDirectory();
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "waits-128.txt");
        var lines = new List<string> { "wait_type before_ms after_ms delta_ms" };
        foreach (var pair in after)
        {
            before.TryGetValue(pair.Key, out var start);
            lines.Add(pair.Key + " " + start.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " " + pair.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " " + (pair.Value - start).ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        lines.Add("samples");
        foreach (var pair in hits.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            lines.Add(pair.Key + " " + pair.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await File.WriteAllLinesAsync(path, lines, CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task PollAsync(
        string connectionString,
        Dictionary<string, int> hits,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        const string sql = """
            SELECT
                wt.wait_type,
                wt.resource_description
            FROM sys.dm_os_waiting_tasks AS wt
            WHERE wt.wait_type LIKE N'PAGELATCH%'
            """;
        while (!cancellationToken.IsCancellationRequested)
        {
            await using var command = new SqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var wait = reader.GetString(0);
                var resource = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var database = DatabaseOf(resource);
                var key = wait + " " + database;
                lock (hits)
                    hits[key] = hits.TryGetValue(key, out var count) ? count + 1 : 1;
            }

            await Task.Delay(20, cancellationToken).ConfigureAwait(false);
        }
    }

    private static string DatabaseOf(string resource)
    {
        // PAGELATCH resources look like "2:1:128" or "PAGE: 2:1:128". 2 is tempdb.
        var digits = new string(resource.SkipWhile(c => !char.IsDigit(c)).TakeWhile(c => char.IsDigit(c) || c == ':').ToArray());
        var first = digits.Split(':', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        return first switch
        {
            "2" => "tempdb",
            null or "" => "unknown",
            _ => "db" + first
        };
    }

    private static async Task<Dictionary<string, long>> ReadWaitsAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT wait_type, wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE wait_type LIKE N'PAGELATCH%'
            """;
        var waits = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            waits[reader.GetString(0)] = reader.GetInt64(1);
        return waits;
    }
}
