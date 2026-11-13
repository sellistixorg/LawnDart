using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace LawnDart.SqlPerformance;

internal sealed class RunReport
{
    public string Commit { get; set; } = "";
    public string Database { get; set; } = "";
    public long Size { get; set; }
    public int Seed { get; set; }
    public int Tenants { get; set; }
    public int Samples { get; set; }
    public long DatabaseBytes { get; set; }
    public long IndexBytes { get; set; }
    public List<CaseResult> Cases { get; set; } = [];
}

internal static class Reports
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static string ResultsDirectory()
    {
        var root = Git.Root();
        return Path.Combine(root, "benchmarks", "LawnDart.SqlPerformance", "results");
    }

    public static string CatalogPath(DatabaseKind kind, DatasetPlan plan)
        => Path.Combine(
            ResultsDirectory(),
            "saved",
            Engines.Word(kind) + "-" + plan.Size.ToString(CultureInfo.InvariantCulture) + "-" + plan.Seed.ToString(CultureInfo.InvariantCulture),
            "catalog.json");

    public static void Write(RunReport report, string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, JsonSerializer.Serialize(report, Json));
    }

    public static RunReport Read(string path)
        => JsonSerializer.Deserialize<RunReport>(File.ReadAllText(path), Json)
           ?? throw new InvalidOperationException("The result file is empty.");

    public static void Print(RunReport report)
    {
        Console.WriteLine(report.Database + " " + report.Commit);
        foreach (var item in report.Cases)
        {
            Console.WriteLine(
                item.Workload + " | " + item.Name
                + " | count " + item.Count.ToString(CultureInfo.InvariantCulture)
                + " | mean " + item.MeanMilliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " | p50 " + item.P50Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " | p95 " + item.P95Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " | p99 " + item.P99Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " | throughput " + item.ThroughputPerSecond.ToString("F3", CultureInfo.InvariantCulture));
        }
    }

    public static void Compare(RunReport left, RunReport right, string path)
    {
        if (!left.Database.Equals(right.Database, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Compare two runs of the same database.");
        if (left.Size != right.Size)
            throw new InvalidOperationException("Compare two runs of the same size.");

        var lines = new List<string>
        {
            left.Database,
            "left " + left.Commit,
            "right " + right.Commit
        };
        var rightByName = right.Cases.ToDictionary(item => item.Workload + "|" + item.Name, StringComparer.Ordinal);
        foreach (var item in left.Cases)
        {
            var key = item.Workload + "|" + item.Name;
            lines.Add(item.Workload + " | " + item.Name);
            lines.Add("left count " + item.Count.ToString(CultureInfo.InvariantCulture)
                + " mean " + item.MeanMilliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " p50 " + item.P50Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " p95 " + item.P95Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " p99 " + item.P99Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " throughput " + item.ThroughputPerSecond.ToString("F3", CultureInfo.InvariantCulture));
            if (!rightByName.TryGetValue(key, out var other))
            {
                lines.Add("right missing");
                continue;
            }

            lines.Add("right count " + other.Count.ToString(CultureInfo.InvariantCulture)
                + " mean " + other.MeanMilliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " p50 " + other.P50Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " p95 " + other.P95Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " p99 " + other.P99Milliseconds.ToString("F3", CultureInfo.InvariantCulture)
                + " throughput " + other.ThroughputPerSecond.ToString("F3", CultureInfo.InvariantCulture));
        }

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllLines(path, lines);
        foreach (var line in lines)
            Console.WriteLine(line);
    }
}

internal static class Git
{
    public static string Root()
    {
        var output = Run("rev-parse", "--show-toplevel");
        return output.Trim();
    }

    public static string Head()
    {
        var output = Run("rev-parse", "HEAD");
        return output.Trim();
    }

    public static string Resolve(string name)
    {
        var output = Run("rev-parse", "--verify", name);
        return output.Trim();
    }

    public static string Worktree(string sha)
    {
        var path = Path.Combine(Path.GetTempPath(), "sqlperf-" + sha);
        if (!Directory.Exists(path))
            Run("worktree", "add", "--detach", path, sha);
        return path;
    }

    public static void RemoveWorktree(string path)
    {
        if (!Directory.Exists(path))
            return;
        Run("worktree", "remove", "--force", path);
    }

    private static string Run(params string[] args)
    {
        var start = new ProcessStartInfo("git")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = FindRepo()
        };
        foreach (var arg in args)
            start.ArgumentList.Add(arg);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start git.");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException("Git could not " + args[0] + ". " + stderr.Trim());
        return stdout;
    }

    private static string FindRepo()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, ".git")) || File.Exists(Path.Combine(dir.FullName, ".git")))
                return dir.FullName;
            dir = dir.Parent;
        }

        return Directory.GetCurrentDirectory();
    }
}
