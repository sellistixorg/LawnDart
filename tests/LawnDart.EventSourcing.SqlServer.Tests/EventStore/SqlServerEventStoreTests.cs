using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Testcontainers.MsSql;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

[Trait("Category", "Integration")]
public class SqlServerEventStoreTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlContainer;
    private SqlServerEventStore? _eventStore;
    private string? _connectionString;
    private readonly string _tableName = $"Events_{Guid.NewGuid():N}";

    public SqlServerEventStoreTests()
    {
        _sqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("Test123!")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _sqlContainer.StartAsync();
        _connectionString = _sqlContainer.GetConnectionString();
        
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        var logger = loggerFactory.CreateLogger<SqlServerEventStore>();
        
        _eventStore = new SqlServerEventStore(_connectionString, null, _tableName, "Streams", true, new SqlServerEventStoreOptions { RequireTenantId = false }, null, logger);
        await _eventStore.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        await _sqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task ReadStreamAsync_EmptyStream_ReturnsEmpty()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";

        // Act
        var result = await _eventStore!.ReadStreamAsync(streamId);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task AppendAsync_ThenReadStream_ReturnsEvents()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";
        var events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        // Act
        await _eventStore!.AppendAsync(streamId, events);
        var result = await _eventStore.ReadStreamAsync(streamId);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);  // First event is version 1 (1-based versioning)
        Assert.Equal(2, result[1].Version);
    }

    [Fact]
    public async Task AppendAsync_WithExpectedVersion_ValidatesConcurrency()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Act
        await _eventStore!.AppendAsync(streamId, events1, expectedVersion: null);
        await _eventStore.AppendAsync(streamId, events2, expectedVersion: 1);  // After first event, current version is 1

        // Assert
        var result = await _eventStore.ReadStreamAsync(streamId);
        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task AppendAsync_WithWrongExpectedVersion_ThrowsConcurrencyException()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Act
        await _eventStore!.AppendAsync(streamId, events1, expectedVersion: null);

        // Assert
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => _eventStore.AppendAsync(streamId, events2, expectedVersion: 5));
    }

    [Fact]
    public async Task AppendAsync_PropagatesMetadata()
    {
        // Arrange
        var streamId = "tenant456:TestAggregate:test-stream";
        var events = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var metadata = new EventMetadata
        {
            UserId = "user123",
            TenantId = "tenant456",
            CorrelationId = "corr-123"
        };

        // Act
        await _eventStore!.AppendAsync(streamId, events, metadata: metadata);
        var result = await _eventStore.ReadStreamAsync(streamId);

        // Assert
        Assert.Single(result);
        Assert.Equal("user123", result[0].Metadata.UserId);
        Assert.Equal("tenant456", result[0].Metadata.TenantId);
        Assert.Equal("corr-123", result[0].Metadata.CorrelationId);
    }

    [Fact]
    public async Task AppendAsync_WithTags_StoresTags()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";
        var events = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var tags = new[] { "tag1", "tag2" };

        // Act
        await _eventStore!.AppendAsync(streamId, events, tags: tags);
        var result = await _eventStore.ReadStreamAsync(streamId);

        // Assert
        Assert.Single(result);
        Assert.Contains("tag1", result[0].Tags);
        Assert.Contains("tag2", result[0].Tags);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToVersion_LimitsResults()
    {
        var streamId = "test-tenant:TestAggregate:to-version-stream";
        var events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };
        await _eventStore!.AppendAsync(streamId, events);

        var result = await _eventStore.ReadStreamAsync(streamId, fromVersion: 0, toVersion: 2);

        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);
        Assert.Equal(2, result[1].Version);
    }

    [Fact]
    public async Task ReadStreamEnumerableAsync_YieldsEventsInOrder()
    {
        var streamId = "test-tenant:TestAggregate:enum-stream";
        var events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };
        await _eventStore!.AppendAsync(streamId, events);

        var collected = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadStreamEnumerableAsync(streamId))
        {
            collected.Add(evt);
        }

        Assert.Equal(3, collected.Count);
        Assert.Equal(1, collected[0].Version);
        Assert.Equal(2, collected[1].Version);
        Assert.Equal(3, collected[2].Version);
    }

    [Fact]
    public async Task ReadStreamEnumerableAsync_WithToVersion_RespectsLimit()
    {
        var streamId = "test-tenant:TestAggregate:enum-tover-stream";
        var events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };
        await _eventStore!.AppendAsync(streamId, events);

        var collected = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadStreamEnumerableAsync(streamId, toVersion: 2))
        {
            collected.Add(evt);
        }

        Assert.Equal(2, collected.Count);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToTimestamp_LimitsResults()
    {
        var streamId = "test-tenant:TestAggregate:to-timestamp-stream";
        var t1 = new DateTime(2025, 2, 1, 10, 0, 0, DateTimeKind.Utc);
        var t2 = new DateTime(2025, 2, 1, 11, 0, 0, DateTimeKind.Utc);
        var t3 = new DateTime(2025, 2, 1, 12, 0, 0, DateTimeKind.Utc);
        await _eventStore!.AppendAsync(streamId, new IEvent[] { new TestEvent(Guid.NewGuid(), t1) }, metadata: new EventMetadata { Timestamp = t1 });
        await _eventStore.AppendAsync(streamId, new IEvent[] { new TestEvent(Guid.NewGuid(), t2) }, metadata: new EventMetadata { Timestamp = t2 });
        await _eventStore.AppendAsync(streamId, new IEvent[] { new TestEvent(Guid.NewGuid(), t3) }, metadata: new EventMetadata { Timestamp = t3 });

        var result = await _eventStore.ReadStreamAsync(streamId, fromVersion: 0, toTimestamp: t2);

        Assert.Equal(2, result.Count);
        Assert.True(result.All(e => e.Metadata.Timestamp <= t2));
    }

    [Fact]
    public async Task ReadByQueryAsync_WithTagFilter_ReturnsMatchingEvents()
    {
        // Arrange
        var streamId1 = "test-tenant:Order:stream1";
        var streamId2 = "test-tenant:Customer:stream2";
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        await _eventStore!.AppendAsync(streamId1, events1, tags: new[] { "order:123" });
        await _eventStore.AppendAsync(streamId2, events2, tags: new[] { "customer:456" });

        var query = Query.FromItems(QueryItem.ByTags("order:123"));

        // Act
        var result = await _eventStore.ReadByQueryAsync(query);

        // Assert
        Assert.Single(result.Events);
        Assert.Contains("order:123", result.Events[0].Tags);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithTypeFilter_ReturnsMatchingEvents()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";
        IEvent[] events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        await _eventStore!.AppendAsync(streamId, events);
        var query = Query.FromItems(QueryItem.ByType(EventTypeNameResolver.GetName(typeof(TestEvent))));

        // Act
        var result = await _eventStore.ReadByQueryAsync(query);

        // Assert
        Assert.Single(result.Events);
        Assert.IsType<TestEvent>(result.Events[0].Event);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_ValidatesCondition()
    {
        // Arrange
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var query = Query.FromItems(QueryItem.ByType("SomeOtherEvent"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act
        var result = await _eventStore!.AppendAsync(events1, condition);

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_IgnoresEarlierEvents()
    {
        // Arrange: Test that the "After" parameter allows ignoring events before a sequence position
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events3 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append first event and capture its sequence position
        var positions1 = await _eventStore!.AppendAsync("test-tenant:TestAggregate:stream1", events1);
        var lastPosition = positions1.SequencePositions[0];

        // Create condition that checks for TestEvent but only AFTER the first position
        var query = Query.FromItems(QueryItem.ByType(EventTypeNameResolver.GetName(typeof(TestEvent))));
        var condition = AppendCondition.FailIfMatches(query, after: lastPosition);

        // Act - Should succeed because events1 is at lastPosition (not after it)
        var result = await _eventStore.AppendAsync(events3, condition);

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_FailsWhenMatchAfterPosition()
    {
        // Arrange: Test that events after the specified position cause the condition to fail
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events3 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append first event and capture its sequence position
        var positions1 = await _eventStore!.AppendAsync("test-tenant:TestAggregate:stream1", events1);
        var lastPosition = positions1.SequencePositions[0];

        // Create condition that checks for TestEvent but only after first position
        var query = Query.FromItems(QueryItem.ByType(EventTypeNameResolver.GetName(typeof(TestEvent))));
        var condition = AppendCondition.FailIfMatches(query, after: lastPosition);

        // Append second event AFTER the position (should cause failure when we try to append events3)
        await _eventStore.AppendAsync("test-tenant:TestAggregate:stream2", events2);

        // Act & Assert - Should fail because matching event exists after the position
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => _eventStore.AppendAsync(events3, condition));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_ValidatesTagBasedCondition()
    {
        // Arrange: Test DCB condition with tag-based query
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with one tag
        await _eventStore!.AppendAsync("test-tenant:Order:stream1", events1, tags: new[] { "order:12345" });

        // Create condition that checks for different tag
        var query = Query.FromItems(QueryItem.ByTags("order:67890"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act - Should succeed because no events match the tag query
        var result = await _eventStore.AppendAsync(events2, condition, tags: new[] { "order:67890" });

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_FailsWhenTagMatches()
    {
        // Arrange: Test that DCB condition fails when tag matches existing event
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with tag
        await _eventStore!.AppendAsync("test-tenant:Order:stream1", events1, tags: new[] { "order:12345" });

        // Create condition that checks for the same tag
        var query = Query.FromItems(QueryItem.ByTags("order:12345"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert - Should fail because event with matching tag exists
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => _eventStore.AppendAsync(events2, condition, tags: new[] { "order:12345" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_RequiresAllTags()
    {
        // Arrange: Test that multi-tag query requires ALL tags to be present
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with only one tag (missing the second tag)
        await _eventStore!.AppendAsync("test-tenant:Order:stream1", events1, tags: new[] { "order:12345" });

        // Create condition that requires BOTH tags
        var query = Query.FromItems(QueryItem.ByTags("order:12345", "payment:abc"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act - Should succeed because no event has both tags
        var result = await _eventStore.AppendAsync(events2, condition, tags: new[] { "order:12345", "payment:abc" });

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_FailsWhenAllTagsMatch()
    {
        // Arrange: Test that condition fails when all required tags match
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with both tags
        await _eventStore!.AppendAsync("test-tenant:Order:stream1", events1, tags: new[] { "order:12345", "payment:abc" });

        // Create condition that requires both tags
        var query = Query.FromItems(QueryItem.ByTags("order:12345", "payment:abc"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert - Should fail because event with both tags exists
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => _eventStore.AppendAsync(events2, condition, tags: new[] { "order:12345", "payment:abc" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiQueryItem_UsesOrLogic()
    {
        // Arrange: Test that multiple query items use OR logic (matches if ANY item matches)
        var events1 = new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new IEvent[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event matching first query item
        await _eventStore!.AppendAsync("test-tenant:Order:stream1", events1, tags: new[] { "order:12345" });

        // Create query with OR logic (matches if ANY item matches)
        var query = Query.FromItems(
            QueryItem.ByTags("order:12345"),
            QueryItem.ByTags("order:67890")
        );
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert - Should fail because first query item matches
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => _eventStore.AppendAsync(events2, condition, tags: new[] { "order:99999" }));
    }

    [Fact]
    public async Task ReadByQueryAsync_WithDCBTags_ReturnsCrossEntityEvents()
    {
        // Arrange: Test cross-entity querying using tags (DCB pattern)
        // Append events for different entities but same order (cross-entity scenario)
        await _eventStore!.AppendAsync("test-tenant:Order:order-stream", 
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, 
            tags: new[] { "order:12345" });
        
        await _eventStore.AppendAsync("test-tenant:Payment:payment-stream", 
            new IEvent[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) }, 
            tags: new[] { "order:12345", "payment:abc" });
        
        await _eventStore.AppendAsync("test-tenant:Inventory:inventory-stream", 
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, 
            tags: new[] { "order:12345", "product:xyz" });

        // Query for all events related to this order (crosses multiple streams/entities)
        var query = Query.FromItems(QueryItem.ByTags("order:12345"));

        // Act
        var result = await _eventStore.ReadByQueryAsync(query);

        // Assert - Should return all 3 events across different streams
        Assert.Equal(3, result.Events.Count);
        Assert.All(result.Events, e => Assert.Contains("order:12345", e.Tags));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_MultiEntityConsistency_EnforcesAtomicity()
    {
        // Arrange: Test multi-entity consistency enforcement using DCB
        // This simulates an order fulfillment workflow spanning Order, Payment, and Inventory
        var orderId = "order:12345";
        var paymentId = "payment:abc";
        var productId = "product:xyz";

        // Simulate: Read current state (no events exist yet)
        var query = Query.FromItems(
            QueryItem.ByTags(orderId),
            QueryItem.ByTags(paymentId),
            QueryItem.ByTags(productId)
        );
        var lastPosition = 0L;

        // Create condition: fail if any events exist for these entities after lastPosition
        // This ensures atomic consistency across multiple entities
        var condition = AppendCondition.FailIfMatches(query, after: lastPosition);

        // Act - Append multi-entity events atomically
        var workflowEvents = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        var result = await _eventStore!.AppendAsync(
            workflowEvents, 
            condition, 
            tags: new[] { orderId, paymentId, productId });

        // Assert - Both events appended atomically
        Assert.Equal(2, result.SequencePositions.Count);

        // Verify query returns both events (cross-entity query)
        var queryResult = await _eventStore.ReadByQueryAsync(query);
        Assert.Equal(2, queryResult.Events.Count);
    }

    [Fact]
    public async Task AppendAsync_ThenReadStream_PreservesEventProperties()
    {
        // Arrange
        var streamId = "test-tenant:TestAggregate:test-stream";
        var originalId = Guid.NewGuid();
        var originalTimestamp = DateTime.UtcNow;
        var originalProductId = "product-123";
        var originalQuantity = 5;
        
        var originalEvent = new EventWithProperties(
            originalId,
            originalTimestamp,
            originalProductId,
            originalQuantity
        );
        var events = new IEvent[] { originalEvent };

        // Act
        await _eventStore!.AppendAsync(streamId, events);
        var result = await _eventStore.ReadStreamAsync(streamId);

        // Assert
        Assert.Single(result);
        var deserializedEvent = Assert.IsType<EventWithProperties>(result[0].Event);
        
        Assert.Equal(originalId, deserializedEvent.Id);
        Assert.Equal(originalTimestamp, deserializedEvent.Timestamp);
        Assert.Equal(originalProductId, deserializedEvent.ProductId);
        Assert.Equal(originalQuantity, deserializedEvent.Quantity);
    }

    [Fact]
    public async Task AppendAsync_WithDcbCondition_UsesOpaqueDcbStreamId()
    {
        var append = await _eventStore!.AppendAsync(
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("order:opaque"))),
            metadata: new EventMetadata { TenantId = "tenant-opaque" },
            tags: new[] { "order:opaque" });

        var read = await _eventStore.ReadByQueryAsync(Query.All(), fromSequencePosition: append.SequencePositions[0], limit: 1);
        var evt = Assert.Single(read.Events);
        Assert.StartsWith("tenant-opaque:dcb:", evt.StreamId);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithAliasedTypeFilter_UsesEventTypeAlias()
    {
        var streamId = "test-tenant:AliasAggregate:stream";
        await _eventStore!.AppendAsync(streamId, new IEvent[]
        {
            new SqlAliasedEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        });

        var result = await _eventStore.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByType(EventTypeNameResolver.GetName(typeof(SqlAliasedEvent)))));

        Assert.Single(result.Events);
        Assert.IsType<SqlAliasedEvent>(result.Events[0].Event);
    }

    [Fact]
    public async Task GetCurrentSequenceAsync_ReturnsMaxCommittedSequence_NotSequenceAllocatorHead()
    {
        var streamId = "test-tenant:TestAggregate:seq-head";
        await _eventStore!.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
        });

        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var burn = new SqlCommand(
                """
                DECLARE @i INT = 0;
                WHILE @i < 1000
                BEGIN
                    SELECT NEXT VALUE FOR [dbo].[EventSequencePosition];
                    SET @i = @i + 1;
                END
                """,
                connection);
            await burn.ExecuteNonQueryAsync();
        }

        var append = await _eventStore.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
        });

        var lastCommitted = append.SequencePositions[^1];
        Assert.True(lastCommitted > 100, "Expected CACHE jump to produce a sequence gap before the final append");

        var head = await _eventStore.GetCurrentSequenceAsync();
        Assert.Equal(lastCommitted, head);

        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var seqCmd = new SqlCommand(
                """
                SELECT CAST(current_value AS BIGINT)
                FROM sys.sequences
                WHERE name = 'EventSequencePosition' AND schema_id = SCHEMA_ID('dbo')
                """,
                connection);
            var allocatorHead = Convert.ToInt64(await seqCmd.ExecuteScalarAsync());
            Assert.True(allocatorHead >= lastCommitted);
        }
    }

    [Fact]
    public async Task ReadByQueryAsync_WithAndTags_ReturnsIntersection()
    {
        await _eventStore!.AppendAsync(
            "test-tenant:Order:and-1",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "order:and", "only-order" });
        var both = await _eventStore.AppendAsync(
            "test-tenant:Order:and-2",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "order:and", "extra:and" });
        await _eventStore.AppendAsync(
            "test-tenant:Order:and-3",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "extra:and" });

        var result = await _eventStore.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByTags("order:and", "extra:and")));

        var evt = Assert.Single(result.Events);
        Assert.Equal(both.SequencePositions[0], evt.SequencePosition);
        Assert.Contains("order:and", evt.Tags);
        Assert.Contains("extra:and", evt.Tags);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithOrTagItems_ReturnsUnion()
    {
        var a = await _eventStore!.AppendAsync(
            "test-tenant:Order:or-a",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "or:a" });
        var b = await _eventStore.AppendAsync(
            "test-tenant:Order:or-b",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "or:b" });
        await _eventStore.AppendAsync(
            "test-tenant:Order:or-c",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "or:c" });

        var result = await _eventStore.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByTags("or:a"), QueryItem.ByTags("or:b")));

        Assert.Equal(2, result.Events.Count);
        Assert.Equal(
            new[] { a.SequencePositions[0], b.SequencePositions[0] },
            result.Events.Select(e => e.SequencePosition).ToArray());
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_NoMatches_ReturnsZero()
    {
        var max = await _eventStore!.GetMaxSequencePositionAsync(
            Query.FromItems(QueryItem.ByTags("missing:tag")));
        Assert.Equal(0L, max);
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_WithTag_ReturnsMaxWithoutFullReplay()
    {
        await _eventStore!.AppendAsync(
            "test-tenant:Order:max-other",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "max:other" });
        var first = await _eventStore.AppendAsync(
            "test-tenant:Order:max-1",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "max:tag" });
        var last = await _eventStore.AppendAsync(
            "test-tenant:Order:max-2",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "max:tag" });

        var query = Query.FromItems(QueryItem.ByTags("max:tag"));
        var max = await _eventStore.GetMaxSequencePositionAsync(query);

        Assert.Equal(last.SequencePositions[0], max);
        Assert.NotEqual(first.SequencePositions[0], max);
        Assert.Equal(max, await _eventStore.GetMaxSequencePositionAsync(query, fromSequencePosition: first.SequencePositions[0]));
        Assert.Equal(0L, await _eventStore.GetMaxSequencePositionAsync(query, fromSequencePosition: last.SequencePositions[0] + 1));
    }

    [Fact]
    public async Task GetMaxSequencePositionAsync_QueryAll_MatchesStoreHead()
    {
        await _eventStore!.AppendAsync(
            "test-tenant:Order:all-max",
            new IEvent[]
            {
                new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
                new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
            });

        var head = await _eventStore.GetCurrentSequenceAsync();
        var max = await _eventStore.GetMaxSequencePositionAsync(Query.All());
        Assert.Equal(head, max);
    }

    [Fact]
    public async Task ReadByQueryAsync_OpenJsonFallback_StillFiltersByTag()
    {
        var tableName = $"Events_OpenJson_{Guid.NewGuid():N}";
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        var logger = loggerFactory.CreateLogger<SqlServerEventStore>();
        var store = new SqlServerEventStore(
            _connectionString!,
            null,
            tableName,
            $"Streams_{tableName}",
            enableRegistry: true,
            new SqlServerEventStoreOptions { RequireTenantId = false, UseEventTagsTable = false },
            null,
            logger);
        await store.InitializeSchemaAsync();

        await store.AppendAsync(
            "test-tenant:Order:openjson",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "openjson:tag" });
        await store.AppendAsync(
            "test-tenant:Order:openjson-other",
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            tags: new[] { "openjson:other" });

        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags("openjson:tag")));
        Assert.Single(result.Events);
        Assert.Contains("openjson:tag", result.Events[0].Tags);

        var max = await store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags("openjson:tag")));
        Assert.Equal(result.Events[0].SequencePosition, max);
    }

    [EventTypeName("sql-server-event-store-tests.test-event")]
    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
    [EventTypeName("sql-server-event-store-tests.another-event")]
    private record AnotherEvent(Guid Id, DateTime Timestamp) : IEvent;
    [EventTypeName("sql-server-event-store-tests.event-with-properties")]
    private record EventWithProperties(Guid Id, DateTime Timestamp, string ProductId, int Quantity) : IEvent;
    [EventTypeName("tests.sql.alias-event")]
    private record SqlAliasedEvent(Guid Id, DateTime Timestamp) : IEvent;
}

