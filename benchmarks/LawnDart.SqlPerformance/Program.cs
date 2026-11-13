using System.Globalization;

namespace LawnDart.SqlPerformance;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            if (args.Length == 0)
            {
                Usage();
                return 1;
            }

            var flags = ReadFlags(args.AsSpan(1));
            switch (args[0])
            {
                case "self-check":
                    GeneratorChecks.AssertStable();
                    Console.WriteLine("generator check passed");
                    return 0;
                case "check":
                    await CheckAsync(flags, CancellationToken.None).ConfigureAwait(false);
                    return 0;
                case "load":
                    await LoadCommandAsync(flags, CancellationToken.None).ConfigureAwait(false);
                    return 0;
                case "save":
                    await SaveCommandAsync(flags, CancellationToken.None).ConfigureAwait(false);
                    return 0;
                case "restore":
                    await RestoreCommandAsync(flags, CancellationToken.None).ConfigureAwait(false);
                    return 0;
                case "run":
                    await RunCommandAsync(flags, report: true, fullCatchUp: true, CancellationToken.None).ConfigureAwait(false);
                    return 0;
                case "compare":
                    CompareCommand(flags);
                    return 0;
                case "compare-commits":
                    await CompareCommitsAsync(flags, CancellationToken.None).ConfigureAwait(false);
                    return 0;
                default:
                    Usage();
                    return 1;
            }
        }
        catch (Exception ex)
        {
            for (var current = ex; current is not null; current = current.InnerException)
                Console.Error.WriteLine(current.Message);
            try
            {
                var directory = Reports.ResultsDirectory();
                Directory.CreateDirectory(directory);
                File.WriteAllText(Path.Combine(directory, "failure.txt"), ex.ToString());
            }
            catch (Exception)
            {
                // The console already has the message.
            }

            return 1;
        }
    }

    private static async Task CheckAsync(Dictionary<string, string> flags, CancellationToken cancellationToken)
    {
        Console.WriteLine("sql performance check");
        var plan = PlanFrom(flags);
        foreach (var kind in Engines.Parse(Value(flags, "database")))
        {
            var word = Engines.Word(kind);
            Console.WriteLine(word + " load started");
            var endpoint = await Engines.EnsureAsync(kind, plan.Size, cancellationToken).ConfigureAwait(false);
            try
            {
                await Engines.RecreateDatabaseAsync(endpoint, cancellationToken).ConfigureAwait(false);
                await Loader.LoadAsync(endpoint, plan, cancellationToken).ConfigureAwait(false);
                var catalog = new EventGenerator(plan).Catalog();
                catalog.Save(Reports.CatalogPath(kind, plan));
                Console.WriteLine(word + " load finished");
                Console.WriteLine(word + " workloads started");
                await Workloads.RunAsync(endpoint, catalog, samples: 1, fullCatchUp: false, cancellationToken).ConfigureAwait(false);
                Console.WriteLine(word + " workloads finished");
            }
            finally
            {
                Engines.ClearPools();
                await Engines.StopOwnedAsync(endpoint, cancellationToken).ConfigureAwait(false);
            }
        }

        Console.WriteLine("sql performance check passed");
    }

    private static async Task LoadCommandAsync(Dictionary<string, string> flags, CancellationToken cancellationToken)
    {
        var plan = PlanFrom(flags);
        var save = Flag(flags, "save");
        foreach (var kind in Engines.Parse(Value(flags, "database")))
        {
            var word = Engines.Word(kind);
            Console.WriteLine(word + " load started");
            var endpoint = await Engines.EnsureAsync(kind, plan.Size, cancellationToken).ConfigureAwait(false);
            await Engines.RecreateDatabaseAsync(endpoint, cancellationToken).ConfigureAwait(false);
            await Loader.LoadAsync(endpoint, plan, cancellationToken).ConfigureAwait(false);
            new EventGenerator(plan).Catalog().Save(Reports.CatalogPath(kind, plan));
            if (save)
                await Engines.SaveAsync(endpoint, cancellationToken).ConfigureAwait(false);
            Console.WriteLine(word + " load finished");
        }
    }

    private static async Task SaveCommandAsync(Dictionary<string, string> flags, CancellationToken cancellationToken)
    {
        var plan = PlanFrom(flags);
        foreach (var kind in Engines.Parse(Value(flags, "database")))
        {
            var endpoint = await Engines.EnsureAsync(kind, plan.Size, cancellationToken).ConfigureAwait(false);
            await Engines.SaveAsync(endpoint, cancellationToken).ConfigureAwait(false);
            Console.WriteLine(Engines.Word(kind) + " save finished");
        }
    }

    private static async Task RestoreCommandAsync(Dictionary<string, string> flags, CancellationToken cancellationToken)
    {
        var plan = PlanFrom(flags);
        foreach (var kind in Engines.Parse(Value(flags, "database")))
        {
            var endpoint = await Engines.EnsureAsync(kind, plan.Size, cancellationToken).ConfigureAwait(false);
            await Engines.RestoreAsync(endpoint, cancellationToken).ConfigureAwait(false);
            Console.WriteLine(Engines.Word(kind) + " restore finished");
        }
    }

    private static async Task RunCommandAsync(
        Dictionary<string, string> flags,
        bool report,
        bool fullCatchUp,
        CancellationToken cancellationToken)
    {
        var plan = PlanFrom(flags);
        var samples = FlagInt(flags, "samples", 50);
        var label = Value(flags, "label");
        var commit = Value(flags, "commit");
        if (string.IsNullOrWhiteSpace(commit))
            commit = Git.Head();
        var fromSaved = Flag(flags, "from-saved");
        foreach (var kind in Engines.Parse(Value(flags, "database")))
        {
            var endpoint = await Engines.EnsureAsync(kind, plan.Size, cancellationToken).ConfigureAwait(false);
            if (fromSaved)
                await Engines.RestoreAsync(endpoint, cancellationToken).ConfigureAwait(false);
            var catalog = new EventGenerator(plan).Catalog();
            catalog.Save(Reports.CatalogPath(kind, plan));
            var cases = await Workloads.RunAsync(endpoint, catalog, samples, fullCatchUp, cancellationToken).ConfigureAwait(false);
            if (!report)
                continue;

            var sizes = await Engines.SizesAsync(endpoint, cancellationToken).ConfigureAwait(false);
            var run = new RunReport
            {
                Commit = commit,
                Database = Engines.Word(kind),
                Size = plan.Size,
                Seed = plan.Seed,
                Tenants = plan.Tenants,
                Samples = samples,
                DatabaseBytes = sizes.DatabaseBytes,
                IndexBytes = sizes.IndexBytes,
                Cases = cases
            };
            var name = string.IsNullOrWhiteSpace(label) ? commit[..Math.Min(12, commit.Length)] : label;
            var path = Path.Combine(Reports.ResultsDirectory(), Engines.Word(kind) + "-" + name + ".json");
            Reports.Write(run, path);
            Reports.Print(run);
            Console.WriteLine("wrote " + path);
        }
    }

    private static void CompareCommand(Dictionary<string, string> flags)
    {
        var left = Reports.Read(Required(flags, "left"));
        var right = Reports.Read(Required(flags, "right"));
        var path = Path.Combine(Reports.ResultsDirectory(), "compare.txt");
        Reports.Compare(left, right, path);
    }

    private static async Task CompareCommitsAsync(Dictionary<string, string> flags, CancellationToken cancellationToken)
    {
        var baseline = Git.Resolve(Required(flags, "baseline"));
        var candidate = Git.Resolve(Required(flags, "candidate"));
        var baselineTree = Git.Worktree(baseline);
        var candidateTree = Git.Worktree(candidate);
        try
        {
            await RunAgainstTreeAsync(flags, baselineTree, baseline, "baseline", cancellationToken).ConfigureAwait(false);
            await RunAgainstTreeAsync(flags, candidateTree, candidate, "candidate", cancellationToken).ConfigureAwait(false);
            foreach (var kind in Engines.Parse(Value(flags, "database")))
            {
                var word = Engines.Word(kind);
                var left = Reports.Read(Path.Combine(Reports.ResultsDirectory(), word + "-baseline.json"));
                var right = Reports.Read(Path.Combine(Reports.ResultsDirectory(), word + "-candidate.json"));
                Reports.Compare(left, right, Path.Combine(Reports.ResultsDirectory(), word + "-compare.txt"));
            }
        }
        finally
        {
            Git.RemoveWorktree(baselineTree);
            Git.RemoveWorktree(candidateTree);
        }
    }

    private static async Task RunAgainstTreeAsync(
        Dictionary<string, string> flags,
        string tree,
        string commit,
        string label,
        CancellationToken cancellationToken)
    {
        var project = Path.Combine(Git.Root(), "benchmarks", "LawnDart.SqlPerformance", "LawnDart.SqlPerformance.csproj");
        var start = new System.Diagnostics.ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Git.Root()
        };
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--project");
        start.ArgumentList.Add(project);
        start.ArgumentList.Add("-c");
        start.ArgumentList.Add("Release");
        start.ArgumentList.Add("-p:LawnDartRoot=" + tree);
        start.ArgumentList.Add("--");
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--from-saved");
        start.ArgumentList.Add("--label");
        start.ArgumentList.Add(label);
        start.ArgumentList.Add("--commit");
        start.ArgumentList.Add(commit);
        if (flags.TryGetValue("database", out var database))
        {
            start.ArgumentList.Add("--database");
            start.ArgumentList.Add(database);
        }

        if (flags.TryGetValue("size", out var size))
        {
            start.ArgumentList.Add("--size");
            start.ArgumentList.Add(size);
        }

        if (flags.TryGetValue("seed", out var seed))
        {
            start.ArgumentList.Add("--seed");
            start.ArgumentList.Add(seed);
        }

        if (flags.TryGetValue("tenants", out var tenants))
        {
            start.ArgumentList.Add("--tenants");
            start.ArgumentList.Add(tenants);
        }

        if (flags.TryGetValue("samples", out var samples))
        {
            start.ArgumentList.Add("--samples");
            start.ArgumentList.Add(samples);
        }

        _ = cancellationToken;
        using var process = System.Diagnostics.Process.Start(start) ?? throw new InvalidOperationException("Could not start the harness.");
        await process.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);
        if (process.ExitCode != 0)
            throw new InvalidOperationException("The harness run for " + label + " failed.");
    }

    private static DatasetPlan PlanFrom(Dictionary<string, string> flags)
    {
        var size = flags.TryGetValue("size", out var text) ? SizeParser.Parse(text) : DatasetPlan.OneMillion;
        var seed = FlagInt(flags, "seed", DatasetPlan.DefaultSeed);
        var tenants = FlagInt(flags, "tenants", DatasetPlan.DefaultTenants);
        return new DatasetPlan(size, seed, tenants);
    }

    private static Dictionary<string, string> ReadFlags(ReadOnlySpan<string> args)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            var key = args[i];
            if (!key.StartsWith("--", StringComparison.Ordinal) || key.Length < 3)
                throw new ArgumentException("Unexpected argument.");
            key = key[2..];
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                map[key] = args[++i];
            else
                map[key] = "true";
        }

        return map;
    }

    private static bool Flag(Dictionary<string, string> flags, string name)
        => flags.TryGetValue(name, out var value) && !value.Equals("false", StringComparison.OrdinalIgnoreCase);

    private static string? Value(Dictionary<string, string> flags, string name)
        => flags.TryGetValue(name, out var value) ? value : null;

    private static string Required(Dictionary<string, string> flags, string name)
        => flags.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new ArgumentException("Missing --" + name);

    private static int FlagInt(Dictionary<string, string> flags, string name, int fallback)
    {
        if (!flags.TryGetValue(name, out var text))
            return fallback;
        if (!int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            throw new ArgumentException("Could not read --" + name);
        return value;
    }

    private static void Usage()
    {
        Console.WriteLine("Commands: self-check, load, save, restore, run, check, compare, compare-commits.");
        Console.WriteLine("Flags: --database postgres|sqlserver|both --size --seed --tenants --samples --save --from-saved --label --commit --left --right --baseline --candidate.");
        Console.WriteLine("Size accepts an event count, 1M, 100M, or 1B. The default size is 1M.");
        Console.WriteLine("load writes rows. save keeps that database. restore brings it back. run times the four workloads and writes a local result file.");
        Console.WriteLine("check loads and runs each workload without printing timings.");
        Console.WriteLine("compare reads two local result files for the same database. compare-commits runs two commits against the saved database.");
        Console.WriteLine("Set LAWNDART_SQLPERF_PG or LAWNDART_SQLPERF_SQL to use a database that is already running.");
    }
}
