using Testcontainers.MsSql;

namespace LawnDart.Messaging.SqlServer.Tests;

public sealed class SqlInboxFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder(MsSqlTestImage.Server2022)
        .WithPassword("Test123!")
        .Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class SqlInboxCollection : ICollectionFixture<SqlInboxFixture>
{
    public const string Name = "SqlInbox";
}
