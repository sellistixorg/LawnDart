namespace LawnDart.Messaging.Postgres;

/// <summary>
/// Connection and table settings for <see cref="PostgresInboxStore"/>.
/// </summary>
public class PostgresInboxStoreOptions
{
    /// <summary>
    /// Postgres connection string for claims and marks. Required.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Optional owner connection used only to create the inbox table.
    /// When null, <see cref="ConnectionString"/> creates the table.
    /// </summary>
    public string? MigrationConnectionString { get; set; }

    /// <summary>
    /// Schema that holds the inbox table. Defaults to <c>lawndart</c>.
    /// Created by <see cref="PostgresInboxStore.InitializeSchemaAsync"/> when it is missing.
    /// The <c>public</c> schema is rejected.
    /// </summary>
    public string SchemaName { get; set; } = "lawndart";

    /// <summary>
    /// Inbox table name. Defaults to <c>inbox</c>.
    /// </summary>
    public string TableName { get; set; } = "inbox";
}
