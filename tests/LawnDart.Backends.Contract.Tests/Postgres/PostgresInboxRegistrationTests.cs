using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Postgres;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

public class PostgresInboxRegistrationTests
{
    private const string ConnectionString = "Host=localhost;Username=lawndart;Password=lawndart;Database=lawndart";

    [Fact]
    public void Defaults_are_lawndart_inbox()
    {
        var options = new PostgresInboxStoreOptions();
        Assert.Equal("lawndart", options.SchemaName);
        Assert.Equal("inbox", options.TableName);
        Assert.Equal(string.Empty, options.ConnectionString);
    }

    [Fact]
    public void Public_schema_is_rejected()
    {
        var ex = Assert.Throws<ArgumentException>(() => new PostgresInboxStore(
            Options.Create(new PostgresInboxStoreOptions
            {
                ConnectionString = ConnectionString,
                SchemaName = "public"
            }),
            Options.Create(new MessagingOptions())));
        Assert.Contains("public", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AddPostgresInboxStore_replaces_in_memory_inbox_in_either_order(bool postgresFirst)
    {
        var services = new ServiceCollection();
        if (postgresFirst)
        {
            services.AddPostgresInboxStore(ConnectionString);
            services.AddInMemoryMessaging();
        }
        else
        {
            services.AddInMemoryMessaging();
            services.AddPostgresInboxStore(ConnectionString);
        }

        using var provider = services.BuildServiceProvider();
        var inbox = provider.GetRequiredService<IInboxStore>();
        var store = Assert.IsType<PostgresInboxStore>(inbox);
        Assert.Equal("\"lawndart\".\"inbox\"", store.QualifiedTableName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddPostgresInboxStore_rejects_missing_connection_string(string? connectionString)
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() => services.AddPostgresInboxStore(connectionString!));
        Assert.Equal("connectionString", ex.ParamName);
    }
}
