using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventStore;
using LawnDart.Demo.Academy.Projections;
using LawnDart.Demo.Academy.Showcases;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.EDA;
using LawnDart.Metadata;

namespace LawnDart.Demo.Academy;

/// <summary>
/// LawnDart Academy — the definitive pattern showcase.
/// Runs zero-infrastructure by default (in-memory event store).
///
/// Usage:
///   dotnet run                  Interactive menu (UseInMemory)
///   dotnet run -- --run-all     Run every showcase non-interactively (CI / smoke-test)
///   dotnet run -- --sql         Use SQL Server (requires ConnectionStrings:Academy)
///   dotnet run -- --sql --run-all
/// </summary>
internal class Program
{
    private static bool _isInteractive;

    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        var runAll = args.Contains("--run-all");
        var useSql = args.Contains("--sql");
        _isInteractive = !runAll && Environment.UserInteractive && !Console.IsInputRedirected;

        PrintBanner();

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));

        services.AddLawnDart(options =>
        {
            options.RequireTenantId = false;
            options.EnableAuthorization = false;
        });

        services.AddSingleton<ITenantContextProvider>(
            new DemoTenantContextProvider("academy-tenant"));

        var academyCtx = services.AddBoundedContext("default")
            .WithEventTypes(typeof(StudentRegistered).Assembly);
        if (useSql)
        {
            var connectionString =
                config.GetConnectionString("Academy")
                ?? Environment.GetEnvironmentVariable("LAWNDART_SQL_CONNECTION");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine(" SQL mode requires ConnectionStrings:Academy or LAWNDART_SQL_CONNECTION.");
                Environment.ExitCode = 1;
                return;
            }

            academyCtx.UseSqlServer(opts =>
            {
                opts.ConnectionString = connectionString;
                opts.RequireTenantId = false;
            });
        }
        else
        {
            academyCtx.UseInMemory();
        }

        services.AddSingleton<CourseSectionProjector>();
        services.AddSingleton<StudentTranscriptProjector>();

        services.AddTransient<ShowcaseA_TraditionalEda>();
        services.AddTransient<ShowcaseB_DcbInProcess>();
        services.AddTransient<ShowcaseC_LiveProjectionPanel>();
        services.AddTransient<ShowcaseD_Performance>();
        services.AddTransient<ShowcaseE_TemporalQueries>();

        var sp = services.BuildServiceProvider();

        Console.WriteLine(useSql
            ? " Event store: SQL Server"
            : " Event store: InMemory (zero infrastructure)\n");

        if (runAll)
            await RunAllAsync(sp);
        else
            await RunMenuAsync(sp);
    }

    static async Task RunAllAsync(IServiceProvider sp)
    {
        Console.WriteLine("=== Running all showcases non-interactively ===\n");

        await RunShowcaseA(sp);
        await RunShowcaseB(sp);
        await RunSideBySide(sp);
        await RunShowcaseC(sp);
        await RunShowcaseD(sp);
        await RunShowcaseE(sp);
        RunOverdueRegistrationDemo();

        Console.WriteLine("\n=== All showcases completed successfully ===");
    }

    static async Task RunMenuAsync(IServiceProvider sp)
    {
        while (true)
        {
            PrintMenu();
            Console.Write(" Enter choice: ");
            var input = Console.ReadLine()?.Trim().ToLowerInvariant();

            switch (input)
            {
                case "1":
                    await RunShowcaseA(sp);
                    break;

                case "2":
                    await RunShowcaseB(sp);
                    break;

                case "vs" or "1v2" or "compare":
                    await RunSideBySide(sp);
                    break;

                case "3":
                    await RunShowcaseC(sp);
                    break;

                case "4":
                    await RunShowcaseD(sp);
                    break;

                case "5":
                    RunOverdueRegistrationDemo();
                    break;

                case "6":
                    await RunShowcaseE(sp);
                    break;

                case "0" or "q" or "exit" or "quit" or null:
                    Console.WriteLine("\n Goodbye!\n");
                    return;

                default:
                    Console.WriteLine("\n  Invalid choice.");
                    break;
            }

            if (_isInteractive)
            {
                Console.WriteLine("\n Press any key to return to menu...");
                Console.ReadKey(intercept: true);
            }
        }
    }

    static void SafeClear()
    {
        if (_isInteractive)
        {
            try { Console.Clear(); } catch { /* ignore in piped/CI environments */ }
        }
        else
        {
            Console.WriteLine();
        }
    }

    static void SetColor(ConsoleColor color)
    {
        if (_isInteractive)
            try { Console.ForegroundColor = color; } catch { }
    }

    static void ResetColor()
    {
        if (_isInteractive)
            try { Console.ResetColor(); } catch { }
    }

    static void PrintBanner()
    {
        SafeClear();
        SetColor(ConsoleColor.Cyan);
        Console.WriteLine("""
 ╔═══════════════════════════════════════════════════════════════════════╗
 ║                       LAWNDART ACADEMY                                ║
 ║              Messaging Pattern Showcase  •  .NET 10                   ║
 ╚═══════════════════════════════════════════════════════════════════════╝
""");
        ResetColor();
    }

    static void PrintMenu()
    {
        SafeClear();
        PrintBanner();
        SetColor(ConsoleColor.Yellow);
        Console.WriteLine(" Pattern Showcases");
        Console.WriteLine($" {'─',69}");
        ResetColor();
        Console.WriteLine("  [1]   Traditional Aggregate Root + EDA Choreography");
        Console.WriteLine("        Stream-per-aggregate, broker-driven, eventual consistency");
        Console.WriteLine();
        Console.WriteLine("  [2]   DCB In-Process (same workflow, no broker)");
        Console.WriteLine("        Tag-based consistency, atomic append, strong consistency");
        Console.WriteLine();
        Console.WriteLine("  [vs]  Side-by-side Comparison — [1] vs [2] with commentary");
        Console.WriteLine();
        Console.WriteLine("  [3]   Live Projection Panel");
        Console.WriteLine("        10 students racing for 5 seats — real-time dashboard");
        Console.WriteLine();
        Console.WriteLine("  [4]   InMemory Throughput Demo");
        Console.WriteLine("        Batch appends — measure in-process event store throughput");
        Console.WriteLine();
        Console.WriteLine("  [5]   Overdue Registration Task Processor");
        Console.WriteLine("        State → Command pattern for scheduled cleanup");
        Console.WriteLine();
        Console.WriteLine("  [6]   Temporal Queries");
        Console.WriteLine("        Point-in-time state, ReadStreamEnumerableAsync");
        Console.WriteLine();
        SetColor(ConsoleColor.DarkGray);
        Console.WriteLine("  [0]   Exit");
        ResetColor();
        Console.WriteLine();
    }

    static async Task RunShowcaseA(IServiceProvider sp)
    {
        SafeClear();
        PrintShowcaseHeader("Showcase A: Traditional Aggregate Root + EDA Choreography");
        Console.WriteLine("""
  Architecture: Stream-per-aggregate (Student, Course, CourseSection own streams)
  Consistency:  Eventual — seat reservation and payment are separate transactions
  Broker:       In-memory transport (InMemoryMessageTransport)
  Hops:         5 round-trips total
""");

        var showcase = sp.GetRequiredService<ShowcaseA_TraditionalEda>();
        var result = await showcase.RunAsync(
            studentId: Guid.NewGuid(),
            courseId: Guid.NewGuid(),
            sectionId: Guid.NewGuid(),
            studentName: "Alice Nguyen");

        Console.WriteLine(" Execution steps:");
        foreach (var step in result.Steps)
            Console.WriteLine($"  {step}");

        Console.WriteLine();
        PrintResultSummary("A", result.Completed, result.TotalRoundTrips, result.ConsistencyModel, result.ElapsedMs);
    }

    static async Task RunShowcaseB(IServiceProvider sp)
    {
        SafeClear();
        PrintShowcaseHeader("Showcase B: DCB In-Process (no broker)");
        Console.WriteLine("""
  Architecture: Tag-based DCB — events tagged with student:{id} + section:{id}
  Consistency:  Strong — SeatReserved + StudentEnrolled appended atomically
  Broker:       None required
  Hops:         3 round-trips (vs 5 in Showcase A)
""");

        var showcase = sp.GetRequiredService<ShowcaseB_DcbInProcess>();
        var result = await showcase.RunAsync(
            studentId: Guid.NewGuid(),
            courseId: Guid.NewGuid(),
            sectionId: Guid.NewGuid(),
            studentName: "Bob Okafor");

        Console.WriteLine(" Execution steps:");
        foreach (var step in result.Steps)
            Console.WriteLine($"  {step}");

        Console.WriteLine();
        PrintResultSummary("B", result.Completed, result.TotalRoundTrips, result.ConsistencyModel, result.ElapsedMs);
    }

    static async Task RunSideBySide(IServiceProvider sp)
    {
        SafeClear();
        PrintShowcaseHeader("Side-by-Side Comparison: Showcase A vs B");

        var courseIdA = Guid.NewGuid();
        var courseIdB = Guid.NewGuid();
        var sectionIdA = Guid.NewGuid();
        var sectionIdB = Guid.NewGuid();

        Console.WriteLine(" Running both showcases in parallel (independent courses)...\n");

        var showcaseA = sp.GetRequiredService<ShowcaseA_TraditionalEda>();
        var resultA = await showcaseA.RunAsync(Guid.NewGuid(), courseIdA, sectionIdA, "Alice Nguyen (A)");

        var showcaseB = sp.GetRequiredService<ShowcaseB_DcbInProcess>();
        var resultB = await showcaseB.RunAsync(Guid.NewGuid(), courseIdB, sectionIdB, "Bob Okafor (B)");

        Console.WriteLine();
        SetColor(ConsoleColor.Yellow);
        Console.WriteLine(" ┌─────────────────────────────────────┬──────────────────────────────────────┐");
        Console.WriteLine(" │  Showcase A: EDA Choreography       │  Showcase B: DCB In-Process          │");
        Console.WriteLine(" │  (Traditional + MessageTransport)   │  (Single process, no broker)         │");
        Console.WriteLine(" ├─────────────────────────────────────┼──────────────────────────────────────┤");
        ResetColor();

        var aSteps = resultA.Steps.Take(5).ToList();
        var bSteps = resultB.Steps.Take(5).ToList();
        var maxRows = Math.Max(aSteps.Count, bSteps.Count);
        for (var i = 0; i < maxRows; i++)
        {
            var aCell = i < aSteps.Count ? Truncate(aSteps[i], 36) : string.Empty;
            var bCell = i < bSteps.Count ? Truncate(bSteps[i], 37) : string.Empty;
            Console.WriteLine($" │  {aCell,-36}│  {bCell,-37}│");
        }

        SetColor(ConsoleColor.Yellow);
        Console.WriteLine(" ├─────────────────────────────────────┼──────────────────────────────────────┤");
        Console.WriteLine($" │  Round-trips: {resultA.TotalRoundTrips,-22} │  Round-trips: {resultB.TotalRoundTrips,-22} │");
        Console.WriteLine($" │  Consistency: Eventual              │  Consistency: Strong                 │");
        Console.WriteLine($" │  Completed:   {(resultA.Completed ? "Yes" : "No"),-22} │  Completed:   {(resultB.Completed ? "Yes" : "No"),-22} │");
        Console.WriteLine($" │  Elapsed:     {resultA.ElapsedMs} ms{string.Empty,-17}│  Elapsed:     {resultB.ElapsedMs} ms{string.Empty,-17}│");
        Console.WriteLine(" └─────────────────────────────────────┴──────────────────────────────────────┘");
        ResetColor();

        Console.WriteLine("""

 Commentary:
  • Both produce the same end-state: student enrolled, seat reserved, confirmation sent.
  • Showcase A requires a message broker, inbox deduplication, and eventual consistency.
  • Showcase B uses a single DCB append — atomically consistent, no broker needed.
  • Use Showcase A when you need cross-system choreography (separate services, microservices).
  • Use Showcase B when enrollment logic lives in one process (single service, modular monolith).
""");
    }

    static async Task RunShowcaseC(IServiceProvider sp)
    {
        SafeClear();
        PrintShowcaseHeader("Showcase C: Live Projection Panel");

        var showcase = sp.GetRequiredService<ShowcaseC_LiveProjectionPanel>();
        await showcase.RunAsync(totalStudents: 10, totalSeats: 5);
    }

    static async Task RunShowcaseD(IServiceProvider sp)
    {
        SafeClear();
        PrintShowcaseHeader("Showcase D: InMemory Throughput Demo");

        var showcase = sp.GetRequiredService<ShowcaseD_Performance>();
        await showcase.RunAsync(batchSize: 10_000);
    }

    static async Task RunShowcaseE(IServiceProvider sp)
    {
        SafeClear();
        PrintShowcaseHeader("Showcase E: Temporal Queries and IAsyncEnumerable");
        Console.WriteLine("""
  APIs:     ReadStreamAsync(streamId, toTimestamp: asOf)
            ReadStreamEnumerableAsync(streamId) — stream events one-by-one
  Scenario: Course with price updates; query state at different points in time.
""");

        var showcase = sp.GetRequiredService<ShowcaseE_TemporalQueries>();
        await showcase.RunAsync();
    }

    static void RunOverdueRegistrationDemo()
    {
        SafeClear();
        PrintShowcaseHeader("Showcase 5: Overdue Registration Task Processor (State -> Command)");
        Console.WriteLine("""
  Pattern:  Task Processor — polls state, emits commands when condition is met.
  Scenario: 3 students registered but never paid. System cancels after 24h.
""");

        var pendingRegistrations = new[]
        {
            new PendingRegistration(Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow.AddHours(-25), TimeSpan.FromHours(24)),
            new PendingRegistration(Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow.AddHours(-30), TimeSpan.FromHours(24)),
            new PendingRegistration(Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow.AddMinutes(-30), TimeSpan.FromHours(24)),
        };

        var processor = new OverdueRegistrationProcessor(pendingRegistrations);
        var commands = processor.ProcessTasksAsync().GetAwaiter().GetResult().ToList();

        Console.WriteLine($" Scanned {pendingRegistrations.Length} pending registrations:");
        Console.WriteLine($"   Overdue: {commands.Count}  |  Still pending: {pendingRegistrations.Length - commands.Count}\n");

        foreach (var cmd in commands.OfType<Domain.Student.Commands.CancelRegistrationCommand>())
        {
            Console.WriteLine($"  -> CancelRegistrationCommand");
            Console.WriteLine($"      StudentId: {cmd.StudentId}");
            Console.WriteLine($"      Reason:    {cmd.Reason}");
        }

        Console.WriteLine("""

 In production, this processor runs as a hosted service on a configurable interval.
 It queries the read model for pending registrations and dispatches cancellation commands.
 The ITaskProcessor interface keeps scheduling infrastructure separate from business logic.
""");
    }

    static void PrintShowcaseHeader(string title)
    {
        SetColor(ConsoleColor.Cyan);
        Console.WriteLine($"\n {'─',69}");
        Console.WriteLine($"  {title}");
        Console.WriteLine($" {'─',69}\n");
        ResetColor();
    }

    static void PrintResultSummary(string label, bool completed, int roundTrips, string consistency, int elapsedMs)
    {
        var icon = completed ? "[OK]" : "[FAIL]";
        SetColor(completed ? ConsoleColor.Green : ConsoleColor.Red);
        Console.WriteLine($" {icon} Showcase {label} completed successfully");
        ResetColor();
        Console.WriteLine($"     Round-trips:  {roundTrips}");
        Console.WriteLine($"     Consistency:  {consistency}");
        Console.WriteLine($"     Elapsed:      {elapsedMs} ms");
    }

    static string Truncate(string s, int maxLen)
        => s.Length <= maxLen ? s : s[..maxLen];
}

/// <summary>Simple tenant context provider for demo scenarios.</summary>
internal sealed class DemoTenantContextProvider : ITenantContextProvider
{
    private readonly string? _tenantId;

    public DemoTenantContextProvider(string? tenantId) => _tenantId = tenantId;

    public string? GetTenantId() => _tenantId;
    public string GetTenantIdRequired() => _tenantId
        ?? throw new InvalidOperationException("Tenant ID not set.");
}
