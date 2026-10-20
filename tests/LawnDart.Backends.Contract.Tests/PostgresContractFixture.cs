using Npgsql;
using Testcontainers.PostgreSql;

namespace LawnDart.Backends.Contract.Tests;

public sealed class PostgresContractFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _container;

    public string ConnectionString { get; private set; } = string.Empty;

    public IContractBackend Backend { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder(PostgresTestImage.Current)
            .WithDatabase("lawndart")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();
        await _container.StartAsync();

        var admin = _container.GetConnectionString();
        await using (var connection = new NpgsqlConnection(admin))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(
                """
                DO $$ BEGIN
                    CREATE ROLE lawndart_app LOGIN PASSWORD 'lawndart_app' NOSUPERUSER NOCREATEDB NOCREATEROLE;
                EXCEPTION
                    WHEN duplicate_object THEN NULL;
                END $$;
                GRANT CONNECT, CREATE ON DATABASE lawndart TO lawndart_app;
                """,
                connection);
            await command.ExecuteNonQueryAsync();
        }

        var builder = new NpgsqlConnectionStringBuilder(admin)
        {
            Username = "lawndart_app",
            Password = "lawndart_app"
        };
        ConnectionString = builder.ConnectionString;
        Backend = new PostgresContractBackend(ConnectionString);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}

[CollectionDefinition("PostgresContract")]
public sealed class PostgresContractCollection : ICollectionFixture<PostgresContractFixture>;
