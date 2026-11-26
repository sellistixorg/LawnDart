namespace LawnDart.SqlPerformance.Tests;

public sealed class DatabaseNameTests
{
    [Fact]
    public void Recreate_uses_the_database_named_in_the_connection_string()
    {
        var postgres = DatabaseNames.Postgres("Host=127.0.0.1;Username=postgres;Password=postgres;Database=benchdb");
        var sql = DatabaseNames.SqlServer("Server=127.0.0.1;User Id=sa;Password=Test123!;Initial Catalog=benchdb;TrustServerCertificate=True");
        Assert.Equal("benchdb", postgres);
        Assert.Equal("benchdb", sql);

        Assert.Equal("DROP DATABASE IF EXISTS \"benchdb\"", DatabaseNames.PostgresDropSql(postgres));
        Assert.Equal("CREATE DATABASE \"benchdb\"", DatabaseNames.PostgresCreateSql(postgres));
        Assert.Equal(
            "CREATE DATABASE \"lawndart_sqlperf\" TEMPLATE \"benchdb\"",
            DatabaseNames.PostgresCreateFromTemplateSql("lawndart_sqlperf", postgres));

        var recreate = DatabaseNames.SqlServerRecreateScript(sql);
        Assert.Contains("[benchdb]", recreate, StringComparison.Ordinal);
        Assert.DoesNotContain("[lawndart]", recreate, StringComparison.Ordinal);
        Assert.Contains("DB_ID(N'benchdb')", recreate, StringComparison.Ordinal);

        var backup = DatabaseNames.SqlServerBackupScript(sql);
        Assert.Contains("BACKUP DATABASE [benchdb]", backup, StringComparison.Ordinal);
        Assert.DoesNotContain("[lawndart]", backup, StringComparison.Ordinal);

        var restore = DatabaseNames.SqlServerRestoreScript(sql);
        Assert.Contains("RESTORE DATABASE [benchdb]", restore, StringComparison.Ordinal);
        Assert.DoesNotContain("[lawndart]", restore, StringComparison.Ordinal);
    }

    [Fact]
    public void The_harness_database_name_stays_valid()
    {
        var postgres = DatabaseNames.Postgres("Host=127.0.0.1;Username=postgres;Password=postgres;Database=lawndart");
        var sql = DatabaseNames.SqlServer("Server=127.0.0.1;User Id=sa;Password=Test123!;Database=lawndart");
        Assert.Equal("lawndart", postgres);
        Assert.Equal("lawndart", sql);
        Assert.Contains("[lawndart]", DatabaseNames.SqlServerRecreateScript(sql), StringComparison.Ordinal);
        Assert.Contains(EngineNames.SqlBackupPath, DatabaseNames.SqlServerBackupScript(sql), StringComparison.Ordinal);
    }

    [Fact]
    public void Recreate_refuses_a_system_database()
    {
        Assert.Throws<InvalidOperationException>(() =>
            DatabaseNames.Postgres("Host=127.0.0.1;Username=postgres;Password=postgres;Database=postgres"));
        Assert.Throws<InvalidOperationException>(() =>
            DatabaseNames.SqlServer("Server=127.0.0.1;User Id=sa;Password=Test123!;Initial Catalog=master"));
        Assert.Throws<InvalidOperationException>(() => DatabaseNames.Postgres("Host=127.0.0.1;Username=postgres;Password=postgres"));
        Assert.Throws<InvalidOperationException>(() => DatabaseNames.SqlServer("Server=127.0.0.1;User Id=sa;Password=Test123!"));
    }

    [Fact]
    public void Ten_million_raises_the_sql_server_container_cap()
    {
        Assert.Equal("small", EngineMemory.Tier(DatasetPlan.OneMillion));
        Assert.Equal("mid", EngineMemory.Tier(DatasetPlan.TenMillion));
        Assert.Equal("large", EngineMemory.Tier(DatasetPlan.OneHundredMillion));
        Assert.Equal("4g", EngineMemory.DefaultContainer(DatabaseKind.SqlServer, DatasetPlan.OneMillion));
        Assert.Equal("8g", EngineMemory.DefaultContainer(DatabaseKind.SqlServer, DatasetPlan.TenMillion));
        Assert.Equal("16g", EngineMemory.DefaultContainer(DatabaseKind.SqlServer, DatasetPlan.OneHundredMillion));
        Assert.Equal("2048", EngineMemory.SqlLimit(DatasetPlan.OneMillion));
        Assert.Equal("6144", EngineMemory.SqlLimit(DatasetPlan.TenMillion));
        Assert.Equal("12288", EngineMemory.SqlLimit(DatasetPlan.OneHundredMillion));
        Assert.Equal("4g", EngineMemory.DefaultContainer(DatabaseKind.Postgres, DatasetPlan.TenMillion));
        Assert.Equal("12g", EngineMemory.DefaultContainer(DatabaseKind.Postgres, DatasetPlan.OneHundredMillion));
    }

    [Fact]
    public void Identifiers_quote_embedded_marks()
    {
        Assert.Equal("\"bench\"\"db\"", DatabaseNames.QuotePostgres("bench\"db"));
        Assert.Equal("[bench]]db]", DatabaseNames.QuoteSqlServer("bench]db"));
        Assert.Contains("N'o''db'", DatabaseNames.SqlServerRecreateScript("o'db"), StringComparison.Ordinal);
        Assert.Contains("[o'db]", DatabaseNames.SqlServerRecreateScript("o'db"), StringComparison.Ordinal);
    }

    [Fact]
    public void Hundred_million_tier_configures_large_engine_settings()
    {
        Assert.Equal("512MB", EngineMemory.PostgresSharedBuffers(DatasetPlan.OneMillion));
        Assert.Equal("4GB", EngineMemory.PostgresSharedBuffers(DatasetPlan.OneHundredMillion));
        Assert.Equal("4GB", EngineMemory.PostgresMaxWalSize(DatasetPlan.OneMillion));
        Assert.Equal("16GB", EngineMemory.PostgresMaxWalSize(DatasetPlan.OneHundredMillion));
        Assert.Equal("1g", EngineMemory.PostgresShmSize(DatasetPlan.OneMillion));
        Assert.Equal("5g", EngineMemory.PostgresShmSize(DatasetPlan.OneHundredMillion));
        Assert.Equal("8GB", EngineMemory.PostgresEffectiveCacheSize(DatasetPlan.OneHundredMillion));
        Assert.Equal("512MB", EngineMemory.PostgresMaintenanceWorkMem(DatasetPlan.OneHundredMillion));
        Assert.Equal("32MB", EngineMemory.PostgresWorkMem(DatasetPlan.OneHundredMillion));
        Assert.Equal("15min", EngineMemory.PostgresCheckpointTimeout(DatasetPlan.OneHundredMillion));

        var smallScript = DatabaseNames.SqlServerRecreateScript("benchdb", DatasetPlan.OneMillion);
        Assert.DoesNotContain("FILEGROWTH", smallScript, StringComparison.Ordinal);

        var largeScript = DatabaseNames.SqlServerRecreateScript("benchdb", DatasetPlan.OneHundredMillion);
        Assert.Contains("FILEGROWTH", largeScript, StringComparison.Ordinal);
        Assert.Contains("1024MB", largeScript, StringComparison.Ordinal);
        Assert.Contains("512MB", largeScript, StringComparison.Ordinal);
    }
}
