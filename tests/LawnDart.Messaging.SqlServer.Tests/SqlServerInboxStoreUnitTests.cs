using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Messaging.SqlServer.Tests;

public class SqlServerInboxStoreOptionsTests
{
    [Fact]
    public void Defaults_are_dbo_Inbox()
    {
        var options = new SqlServerInboxStoreOptions();
        Assert.Equal("dbo", options.SchemaName);
        Assert.Equal("Inbox", options.TableName);
        Assert.Equal(string.Empty, options.ConnectionString);
    }
}

public class SqlServerInboxStoreIdentifierTests
{
    [Fact]
    public void Qualifies_schema_and_table_that_contain_bracket_and_quote()
    {
        var store = new SqlServerInboxStore(
            Options.Create(new SqlServerInboxStoreOptions
            {
                ConnectionString = "Server=localhost;Database=test;",
                SchemaName = "a]'b",
                TableName = "In]'box"
            }),
            Options.Create(new MessagingOptions()));

        Assert.Equal("[a]]'b].[In]]'box]", store.QualifiedTableName);
        Assert.Equal("a]''b", store.SchemaNameLiteral);
        Assert.Equal("In]''box", store.TableNameLiteral);
    }
}

public class SqlInboxStoreRegistrationTests
{
    private const string ConnectionString = "Server=localhost;Database=test;";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AddSqlInboxStore_replaces_in_memory_inbox_in_either_order(bool sqlFirst)
    {
        var services = new ServiceCollection();
        if (sqlFirst)
        {
            services.AddSqlInboxStore(ConnectionString);
            services.AddInMemoryMessaging();
        }
        else
        {
            services.AddInMemoryMessaging();
            services.AddSqlInboxStore(ConnectionString);
        }

        using var provider = services.BuildServiceProvider();
        var inbox = provider.GetRequiredService<IInboxStore>();
        var store = Assert.IsType<SqlServerInboxStore>(inbox);
        Assert.Equal("[dbo].[Inbox]", store.QualifiedTableName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddSqlInboxStore_rejects_missing_connection_string(string? connectionString)
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() => services.AddSqlInboxStore(connectionString!));
        Assert.Equal("connectionString", ex.ParamName);
    }

    [Fact]
    public void Configure_overrides_schema_and_table()
    {
        var services = new ServiceCollection();
        services.AddSqlInboxStore(ConnectionString, options =>
        {
            options.SchemaName = "ordering";
            options.TableName = "ReactorInbox";
        });

        using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<SqlServerInboxStore>();
        Assert.Equal("[ordering].[ReactorInbox]", store.QualifiedTableName);
    }

    [Fact]
    public async Task InitializeSqlInboxStoreAsync_when_inbox_is_in_memory_throws()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessaging();
        using var provider = services.BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => provider.InitializeSqlInboxStoreAsync());
        Assert.Contains("AddSqlInboxStore", ex.Message, StringComparison.Ordinal);
    }
}
