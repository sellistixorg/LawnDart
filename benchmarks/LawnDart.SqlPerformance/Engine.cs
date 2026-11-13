using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.SqlPerformance;

internal enum DatabaseKind
{
    Postgres,
    SqlServer
}

internal sealed class EngineEndpoint
{
    public required DatabaseKind Kind { get; init; }
    public required string DatabaseConnectionString { get; init; }
    public required string MaintenanceConnectionString { get; init; }
    public string? ContainerName { get; init; }
    public bool OwnedContainer { get; init; }
}

internal static class Images
{
    public static string Postgres =>
        Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_PG_IMAGE") is { Length: > 0 } image
            ? image
            : "postgres:18.1";

    public static string SqlServer =>
        Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_SQL_IMAGE") is { Length: > 0 } image
            ? image
            : "mcr.microsoft.com/mssql/server:2025-CU9-GDR1-ubuntu-22.04";
}

internal static class EngineNames
{
    public const string Database = "lawndart";
    public const string PgSchema = "lawndart";
    public const string SqlSchema = "dbo";
    public const string PgSaved = "lawndart_sqlperf";
    public const string SqlBackupPath = "/var/opt/mssql/backup/lawndart.bak";
    public const string PgContainer = "lawndart-sqlperf-postgres";
    public const string SqlContainer = "lawndart-sqlperf-sqlserver";
    public const string PgVolume = "lawndart-sqlperf-pg";
    public const string SqlVolume = "lawndart-sqlperf-sql";
    public const string PgPassword = "postgres";
    public const string SqlPassword = "Test123!";
}

internal static class DockerCli
{
    public static async Task<string> RunAsync(CancellationToken cancellationToken, params string[] args)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        foreach (var arg in args)
            start.ArgumentList.Add(arg);

        using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start docker.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        var stdout = await stdoutTask.ConfigureAwait(false);
        var stderr = await stderrTask.ConfigureAwait(false);
        if (process.ExitCode != 0)
            throw new InvalidOperationException("Docker could not " + args[0] + ". " + stderr.Trim());
        return stdout.Trim();
    }
}

internal static class Engines
{
    public static IReadOnlyList<DatabaseKind> Parse(string? text)
    {
        var value = string.IsNullOrWhiteSpace(text) ? "both" : text.Trim();
        if (value.Equals("both", StringComparison.OrdinalIgnoreCase))
            return [DatabaseKind.Postgres, DatabaseKind.SqlServer];
        if (value.Equals("postgres", StringComparison.OrdinalIgnoreCase) || value.Equals("pg", StringComparison.OrdinalIgnoreCase))
            return [DatabaseKind.Postgres];
        if (value.Equals("sqlserver", StringComparison.OrdinalIgnoreCase) || value.Equals("sql", StringComparison.OrdinalIgnoreCase))
            return [DatabaseKind.SqlServer];
        throw new ArgumentException("Database must be postgres, sqlserver, or both.");
    }

    public static string Word(DatabaseKind kind) => kind == DatabaseKind.Postgres ? "postgres" : "sqlserver";

    public static async Task<EngineEndpoint> EnsureAsync(DatabaseKind kind, long size, CancellationToken cancellationToken)
    {
        var external = kind == DatabaseKind.Postgres
            ? Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_PG")
            : Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_SQL");
        if (!string.IsNullOrWhiteSpace(external))
            return External(kind, external);

        var large = size >= DatasetPlan.OneHundredMillion;
        var pgMemory = Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_PG_MEMORY");
        var sqlMemoryCap = Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_SQL_MEMORY");
        var profile = (large ? "large" : "small")
            + "-" + (kind == DatabaseKind.Postgres
                ? string.IsNullOrWhiteSpace(pgMemory) ? (large ? "12g" : "4g") : pgMemory
                : string.IsNullOrWhiteSpace(sqlMemoryCap) ? (large ? "16g" : "4g") : sqlMemoryCap);
        var name = kind == DatabaseKind.Postgres ? EngineNames.PgContainer : EngineNames.SqlContainer;
        await EnsureImageAsync(kind, cancellationToken).ConfigureAwait(false);
        var runningProfile = await ContainerProfileAsync(name, cancellationToken).ConfigureAwait(false);
        if (runningProfile is null || !runningProfile.Equals(profile, StringComparison.Ordinal))
        {
            if (runningProfile is not null)
                await DockerCli.RunAsync(cancellationToken, "rm", "-f", name).ConfigureAwait(false);
            await StartAsync(kind, name, profile, large, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var state = await DockerCli.RunAsync(cancellationToken, "inspect", "-f", "{{.State.Running}}", name).ConfigureAwait(false);
            if (!state.Equals("true", StringComparison.OrdinalIgnoreCase))
                await DockerCli.RunAsync(cancellationToken, "start", name).ConfigureAwait(false);
        }

        var port = await PublishedPortAsync(name, kind == DatabaseKind.Postgres ? "5432" : "1433", cancellationToken).ConfigureAwait(false);
        var endpoint = Connect(kind, port);
        await WaitUntilReadyAsync(endpoint, cancellationToken).ConfigureAwait(false);
        if (kind == DatabaseKind.SqlServer)
            await PrepareSqlBackupDirectoryAsync(name, cancellationToken).ConfigureAwait(false);
        return endpoint;
    }

    public static async Task StopOwnedAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        if (!endpoint.OwnedContainer || string.IsNullOrEmpty(endpoint.ContainerName))
            return;

        try
        {
            await DockerCli.RunAsync(cancellationToken, "rm", "-f", endpoint.ContainerName).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // The container is already gone.
        }

        var volume = endpoint.Kind == DatabaseKind.Postgres ? EngineNames.PgVolume : EngineNames.SqlVolume;
        try
        {
            await DockerCli.RunAsync(cancellationToken, "volume", "rm", volume).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // The volume is already gone.
        }
    }

    public static async Task RecreateDatabaseAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        if (endpoint.Kind == DatabaseKind.Postgres)
            await RecreatePostgresAsync(endpoint, cancellationToken).ConfigureAwait(false);
        else
            await RecreateSqlServerAsync(endpoint, cancellationToken).ConfigureAwait(false);
    }

    public static async Task SaveAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        ClearPools();
        if (endpoint.Kind == DatabaseKind.Postgres)
            await SavePostgresAsync(endpoint, cancellationToken).ConfigureAwait(false);
        else
            await SaveSqlServerAsync(endpoint, cancellationToken).ConfigureAwait(false);
    }

    public static async Task RestoreAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        ClearPools();
        if (endpoint.Kind == DatabaseKind.Postgres)
            await RestorePostgresAsync(endpoint, cancellationToken).ConfigureAwait(false);
        else
            await RestoreSqlServerAsync(endpoint, cancellationToken).ConfigureAwait(false);
        ClearPools();
    }

    public static async Task<(long DatabaseBytes, long IndexBytes)> SizesAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        if (endpoint.Kind == DatabaseKind.Postgres)
        {
            await using var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            var database = await ScalarLongAsync(connection, "SELECT pg_database_size(current_database())", cancellationToken).ConfigureAwait(false);
            var indexes = await ScalarLongAsync(connection, """
                SELECT COALESCE(SUM(pg_indexes_size(c.oid)), 0)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'lawndart' AND c.relkind = 'r'
                """, cancellationToken).ConfigureAwait(false);
            return (database, indexes);
        }

        await using var sql = new SqlConnection(endpoint.DatabaseConnectionString);
        await sql.OpenAsync(cancellationToken).ConfigureAwait(false);
        var files = await ScalarLongSqlAsync(sql, "SELECT COALESCE(SUM(CAST(size AS bigint)), 0) * 8192 FROM sys.database_files", cancellationToken).ConfigureAwait(false);
        var index = await ScalarLongSqlAsync(sql, "SELECT COALESCE(SUM(CASE WHEN index_id > 1 THEN used_page_count ELSE 0 END), 0) * 8192 FROM sys.dm_db_partition_stats", cancellationToken).ConfigureAwait(false);
        return (files, index);
    }

    private static EngineEndpoint External(DatabaseKind kind, string connectionString)
    {
        var maintenance = kind == DatabaseKind.Postgres
            ? WithNpgsqlDatabase(connectionString, "postgres")
            : WithSqlDatabase(connectionString, "master");
        return new EngineEndpoint
        {
            Kind = kind,
            DatabaseConnectionString = connectionString,
            MaintenanceConnectionString = maintenance,
            OwnedContainer = false
        };
    }

    private static EngineEndpoint Connect(DatabaseKind kind, int port)
    {
        if (kind == DatabaseKind.Postgres)
        {
            var database = "Host=127.0.0.1;Port=" + port.ToString(CultureInfo.InvariantCulture)
                + ";Username=postgres;Password=" + EngineNames.PgPassword
                + ";Database=" + EngineNames.Database + ";Command Timeout=0;Timeout=30;Maximum Pool Size=200";
            return new EngineEndpoint
            {
                Kind = kind,
                DatabaseConnectionString = database,
                MaintenanceConnectionString = WithNpgsqlDatabase(database, "postgres"),
                ContainerName = EngineNames.PgContainer,
                OwnedContainer = true
            };
        }

        var sql = "Server=127.0.0.1," + port.ToString(CultureInfo.InvariantCulture)
            + ";User Id=sa;Password=" + EngineNames.SqlPassword
            + ";TrustServerCertificate=True;Encrypt=True;Initial Catalog=" + EngineNames.Database
            + ";Connect Timeout=60;Max Pool Size=200";
        return new EngineEndpoint
        {
            Kind = kind,
            DatabaseConnectionString = sql,
            MaintenanceConnectionString = WithSqlDatabase(sql, "master"),
            ContainerName = EngineNames.SqlContainer,
            OwnedContainer = true
        };
    }

    private static async Task EnsureImageAsync(DatabaseKind kind, CancellationToken cancellationToken)
    {
        var image = kind == DatabaseKind.Postgres ? Images.Postgres : Images.SqlServer;
        try
        {
            await DockerCli.RunAsync(cancellationToken, "image", "inspect", image).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            await DockerCli.RunAsync(cancellationToken, "pull", "-q", image).ConfigureAwait(false);
        }
    }

    private static async Task<string?> ContainerProfileAsync(string name, CancellationToken cancellationToken)
    {
        try
        {
            var profile = await DockerCli.RunAsync(
                cancellationToken, "inspect", "-f", "{{index .Config.Labels \"lawndart.sqlperf.profile\"}}", name)
                .ConfigureAwait(false);
            return string.IsNullOrWhiteSpace(profile) ? "" : profile;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private static async Task StartAsync(DatabaseKind kind, string name, string profile, bool large, CancellationToken cancellationToken)
    {
        if (kind == DatabaseKind.Postgres)
        {
            var memory = Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_PG_MEMORY");
            if (string.IsNullOrWhiteSpace(memory))
                memory = large ? "12g" : "4g";
            var buffers = large ? "4GB" : "512MB";
            var wal = large ? "16GB" : "4GB";
            var shm = large ? "5g" : "1g";
            await DockerCli.RunAsync(
                cancellationToken,
                "run", "-d", "--name", name,
                "--label", "lawndart.sqlperf.profile=" + profile,
                "--memory", memory,
                "--shm-size", shm,
                "-e", "POSTGRES_PASSWORD=" + EngineNames.PgPassword,
                "-e", "POSTGRES_USER=postgres",
                "-v", EngineNames.PgVolume + ":/var/lib/postgresql",
                "-p", "127.0.0.1::5432",
                Images.Postgres,
                "-c", "fsync=on",
                "-c", "synchronous_commit=on",
                "-c", "full_page_writes=on",
                "-c", "shared_buffers=" + buffers,
                "-c", "max_wal_size=" + wal,
                "-c", "max_connections=300").ConfigureAwait(false);
            return;
        }

        var sqlMemory = Environment.GetEnvironmentVariable("LAWNDART_SQLPERF_SQL_MEMORY");
        if (string.IsNullOrWhiteSpace(sqlMemory))
            sqlMemory = large ? "16g" : "4g";
        var limit = large ? "12288" : "2048";
        await DockerCli.RunAsync(
            cancellationToken,
            "run", "-d", "--name", name,
            "--label", "lawndart.sqlperf.profile=" + profile,
            "--memory", sqlMemory,
            "-e", "ACCEPT_EULA=Y",
            "-e", "MSSQL_SA_PASSWORD=" + EngineNames.SqlPassword,
            "-e", "MSSQL_PID=Developer",
            "-e", "MSSQL_MEMORY_LIMIT_MB=" + limit,
            "-v", EngineNames.SqlVolume + ":/var/opt/mssql",
            "-p", "127.0.0.1::1433",
            Images.SqlServer).ConfigureAwait(false);
    }

    private static async Task<int> PublishedPortAsync(string name, string containerPort, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 30; attempt++)
        {
            var text = await DockerCli.RunAsync(cancellationToken, "port", name, containerPort + "/tcp").ConfigureAwait(false);
            var colon = text.LastIndexOf(':');
            if (colon > 0 && int.TryParse(text[(colon + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out var port))
                return port;
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
        }

        throw new InvalidOperationException("The database container did not publish a port.");
    }

    private static async Task WaitUntilReadyAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddMinutes(3);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (endpoint.Kind == DatabaseKind.Postgres)
                {
                    await using var connection = new NpgsqlConnection(endpoint.MaintenanceConnectionString);
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                await using var sql = new SqlConnection(endpoint.MaintenanceConnectionString);
                await sql.OpenAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ex is NpgsqlException or SqlException or TimeoutException)
            {
                last = ex;
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
            }
        }

        throw new InvalidOperationException("The database did not accept connections. " + last?.Message);
    }

    private static async Task PrepareSqlBackupDirectoryAsync(string name, CancellationToken cancellationToken)
    {
        await DockerCli.RunAsync(
            cancellationToken,
            "exec", name, "bash", "-c",
            "mkdir -p /var/opt/mssql/backup && chown mssql:mssql /var/opt/mssql/backup").ConfigureAwait(false);
    }

    private static async Task RecreatePostgresAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        ClearPools();
        await using var connection = new NpgsqlConnection(endpoint.MaintenanceConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, TerminateSql(EngineNames.Database), cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "DROP DATABASE IF EXISTS " + EngineNames.Database, cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "CREATE DATABASE " + EngineNames.Database, cancellationToken).ConfigureAwait(false);
    }

    private static async Task RecreateSqlServerAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        ClearPools();
        await using var connection = new SqlConnection(endpoint.MaintenanceConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var sql = """
            IF DB_ID(N'lawndart') IS NOT NULL
            BEGIN
                ALTER DATABASE [lawndart] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [lawndart];
            END
            CREATE DATABASE [lawndart];
            ALTER DATABASE [lawndart] SET RECOVERY SIMPLE;
            """;
        await ExecuteSqlAsync(connection, sql, cancellationToken).ConfigureAwait(false);
    }

    private static async Task SavePostgresAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(endpoint.MaintenanceConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using (var target = new NpgsqlConnection(endpoint.DatabaseConnectionString))
        {
            await target.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(target, "CHECKPOINT", cancellationToken).ConfigureAwait(false);
        }

        ClearPools();
        await ExecuteAsync(connection, TerminateSql(EngineNames.Database), cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, TerminateSql(EngineNames.PgSaved), cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "DROP DATABASE IF EXISTS " + EngineNames.PgSaved, cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "CREATE DATABASE " + EngineNames.PgSaved + " TEMPLATE " + EngineNames.Database, cancellationToken).ConfigureAwait(false);
    }

    private static async Task RestorePostgresAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(endpoint.MaintenanceConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var exists = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM pg_database WHERE datname = '" + EngineNames.PgSaved + "'", cancellationToken).ConfigureAwait(false);
        if (exists < 1)
            throw new InvalidOperationException("There is no saved Postgres database. Load and save first.");

        await ExecuteAsync(connection, TerminateSql(EngineNames.Database), cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "DROP DATABASE IF EXISTS " + EngineNames.Database, cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "CREATE DATABASE " + EngineNames.Database + " TEMPLATE " + EngineNames.PgSaved, cancellationToken).ConfigureAwait(false);
    }

    private static async Task SaveSqlServerAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(endpoint.DatabaseConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteSqlAsync(connection, "CHECKPOINT", cancellationToken).ConfigureAwait(false);
        await using var master = new SqlConnection(endpoint.MaintenanceConnectionString);
        await master.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteSqlAsync(master, "BACKUP DATABASE [lawndart] TO DISK = N'" + EngineNames.SqlBackupPath + "' WITH INIT, FORMAT, COMPRESSION", cancellationToken).ConfigureAwait(false);
    }

    private static async Task RestoreSqlServerAsync(EngineEndpoint endpoint, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(endpoint.MaintenanceConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ExecuteSqlAsync(connection, "RESTORE HEADERONLY FROM DISK = N'" + EngineNames.SqlBackupPath + "'", cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException)
        {
            throw new InvalidOperationException("There is no saved SQL Server backup. Load and save first.");
        }

        var sql = """
            IF DB_ID(N'lawndart') IS NOT NULL
                ALTER DATABASE [lawndart] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            RESTORE DATABASE [lawndart] FROM DISK = N'/var/opt/mssql/backup/lawndart.bak' WITH REPLACE;
            ALTER DATABASE [lawndart] SET MULTI_USER;
            """;
        await ExecuteSqlAsync(connection, sql, cancellationToken).ConfigureAwait(false);
    }

    private static string TerminateSql(string database)
        => "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + database + "' AND pid <> pg_backend_pid()";

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static async Task ExecuteSqlAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<long> ScalarLongSqlAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 0 };
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static string WithNpgsqlDatabase(string connectionString, string database)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Database = database };
        return builder.ConnectionString;
    }

    private static string WithSqlDatabase(string connectionString, string database)
    {
        var builder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = database };
        return builder.ConnectionString;
    }

    public static void ClearPools()
    {
        NpgsqlConnection.ClearAllPools();
        SqlConnection.ClearAllPools();
    }
}

internal static class TextEscape
{
    public static string Copy(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value)
        {
            switch (ch)
            {
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                default:
                    builder.Append(ch);
                    break;
            }
        }

        return builder.ToString();
    }
}
