namespace LawnDart.Messaging.SqlServer;

/// <summary>
/// Connection and table settings for <see cref="SqlServerInboxStore"/>.
/// </summary>
public class SqlServerInboxStoreOptions
{
    /// <summary>
    /// SQL Server connection string. Required.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Schema that holds the inbox table. Defaults to <c>dbo</c>.
    /// Created by <see cref="SqlServerInboxStore.InitializeSchemaAsync"/> when it is missing.
    /// </summary>
    public string SchemaName { get; set; } = "dbo";

    /// <summary>
    /// Inbox table name. Defaults to <c>Inbox</c>.
    /// </summary>
    public string TableName { get; set; } = "Inbox";
}
