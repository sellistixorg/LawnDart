using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Xunit;

namespace LawnDart.EventSourcing.Tests.EventStore;

public class InMemoryEventStoreTests
{
    [Fact]
    public async Task ReadStreamAsync_EmptyStream_ReturnsEmpty()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";

        // Act
        var result = await store.ReadStreamAsync(streamId);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task AppendAsync_ThenReadStream_ReturnsEvents()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events = new[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        // Act
        await store.AppendAsync(streamId, events);
        var result = await store.ReadStreamAsync(streamId);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);
        Assert.Equal(2, result[1].Version);
    }

    [Fact]
    public async Task AppendAsync_WithCorrectExpectedVersion_Succeeds()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Act — empty stream reports current version -1; first event is version 1.
        await store.AppendAsync(streamId, events1, expectedVersion: -1);
        await store.AppendAsync(streamId, events2, expectedVersion: 1);

        // Assert
        var result = await store.ReadStreamAsync(streamId);
        Assert.Equal(2, result.Count);
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(streamId, events2, expectedVersion: 1));
    }

    [Fact]
    public async Task AppendAsync_WithWrongExpectedVersion_ThrowsConcurrencyException()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Act
        await store.AppendAsync(streamId, events1, expectedVersion: -1);
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(streamId, events2, expectedVersion: 5));
    }


    [Fact]
    public async Task ReadByQueryAsync_WithTypeFilter_ReturnsMatchingEvents()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        IEvent[] events = new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow)
        };

        await store.AppendAsync(streamId, events);
        var query = Query.FromItems(QueryItem.ByType(EventTypeCatalog.TryGetDeclaredName(typeof(TestEvent))!));

        // Act
        var result = await store.ReadByQueryAsync(query);

        // Assert
        Assert.Single(result.Events);
        Assert.IsType<TestEvent>(result.Events[0].Event);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithTagFilter_ReturnsMatchingEvents()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var tags = new[] { "tag1", "tag2" };

        await store.AppendAsync(streamId, events, tags: tags);
        var query = Query.FromItems(QueryItem.ByTags("tag1"));

        // Act
        var result = await store.ReadByQueryAsync(query);

        // Assert
        Assert.Single(result.Events);
        Assert.Contains("tag1", result.Events[0].Tags);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_ValidatesCondition()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var events = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var query = Query.FromItems(QueryItem.ByType("SomeOtherEvent"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act
        var result = await store.AppendAsync(events, condition);

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_FailsWhenConditionMatches()
    {
        // Arrange
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        await store.AppendAsync(streamId, events1);
        var query = Query.FromItems(QueryItem.ByType(EventTypeCatalog.TryGetDeclaredName(typeof(TestEvent))!));
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(events2, condition));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_IgnoresEarlierEvents()
    {
        // Arrange: Test that the "After" parameter allows ignoring events before a sequence position
        // This is useful for optimistic concurrency where you want to check for conflicts
        // only after a certain point in the event stream
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events3 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append first event and capture its sequence position
        var positions1 = await store.AppendAsync("stream1", events1);
        var lastPosition = positions1.SequencePositions[0];

        // Create condition that checks for TestEvent but only AFTER the first position
        // This means events at or before lastPosition are ignored
        var query = Query.FromItems(QueryItem.ByType(EventTypeCatalog.TryGetDeclaredName(typeof(TestEvent))!));
        var condition = AppendCondition.FailIfMatches(query, after: lastPosition);

        // Act - Should succeed because events1 is at lastPosition (not after it)
        // and no other TestEvents exist after that position yet
        var result = await store.AppendAsync(events3, condition);

        // Assert - Append succeeds because the condition only checks events after lastPosition
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_FailsWhenMatchAfterPosition()
    {
        // Arrange: Test that events after the specified position cause the condition to fail
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events3 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append first event and capture its sequence position
        var positions1 = await store.AppendAsync("stream1", events1);
        var lastPosition = positions1.SequencePositions[0];

        // Create condition that checks for TestEvent but only after first position
        var query = Query.FromItems(QueryItem.ByType(EventTypeCatalog.TryGetDeclaredName(typeof(TestEvent))!));
        var condition = AppendCondition.FailIfMatches(query, after: lastPosition);

        // Append second event AFTER the position (should cause failure when we try to append events3)
        await store.AppendAsync("stream2", events2);

        // Act & Assert - Should fail because matching event exists after the position
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(events3, condition));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_ValidatesTagBasedCondition()
    {
        // Arrange: Test DCB condition with tag-based query
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with one tag
        await store.AppendAsync("stream1", events1, tags: new[] { "order:12345" });

        // Create condition that checks for different tag
        var query = Query.FromItems(QueryItem.ByTags("order:67890"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act - Should succeed because no events match the tag query
        var result = await store.AppendAsync(events2, condition, tags: new[] { "order:67890" });

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_FailsWhenTagMatches()
    {
        // Arrange: Test that DCB condition fails when tag matches existing event
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with tag
        await store.AppendAsync("stream1", events1, tags: new[] { "order:12345" });

        // Create condition that checks for the same tag
        var query = Query.FromItems(QueryItem.ByTags("order:12345"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert - Should fail because event with matching tag exists
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(events2, condition, tags: new[] { "order:12345" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_RequiresAllTags()
    {
        // Arrange: Test that multi-tag query requires ALL tags to be present
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with only one tag (missing the second tag)
        await store.AppendAsync("stream1", events1, tags: new[] { "order:12345" });

        // Create condition that requires BOTH tags
        var query = Query.FromItems(QueryItem.ByTags("order:12345", "payment:abc"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act - Should succeed because no event has both tags
        var result = await store.AppendAsync(events2, condition, tags: new[] { "order:12345", "payment:abc" });

        // Assert
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_FailsWhenAllTagsMatch()
    {
        // Arrange: Test that condition fails when all required tags match
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event with both tags
        await store.AppendAsync("stream1", events1, tags: new[] { "order:12345", "payment:abc" });

        // Create condition that requires both tags
        var query = Query.FromItems(QueryItem.ByTags("order:12345", "payment:abc"));
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert - Should fail because event with both tags exists
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(events2, condition, tags: new[] { "order:12345", "payment:abc" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiQueryItem_UsesOrLogic()
    {
        // Arrange: Test that multiple query items use OR logic (matches if ANY item matches)
        var store = new InMemoryEventStore();
        var events1 = new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) };
        var events2 = new[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) };

        // Append event matching first query item
        await store.AppendAsync("stream1", events1, tags: new[] { "order:12345" });

        // Create query with OR logic (matches if ANY item matches)
        var query = Query.FromItems(
            QueryItem.ByTags("order:12345"),
            QueryItem.ByTags("order:67890")
        );
        var condition = AppendCondition.FailIfMatches(query);

        // Act & Assert - Should fail because first query item matches
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => store.AppendAsync(events2, condition, tags: new[] { "order:99999" }));
    }

    [Fact]
    public async Task ReadByQueryAsync_WithDCBTags_ReturnsCrossEntityEvents()
    {
        // Arrange: Test cross-entity querying using tags (DCB pattern)
        var store = new InMemoryEventStore();
        
        // Append events for different entities but same order (cross-entity scenario)
        await store.AppendAsync("order-stream", 
            new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, 
            tags: new[] { "order:12345" });
        
        await store.AppendAsync("payment-stream", 
            new[] { new AnotherEvent(Guid.NewGuid(), DateTime.UtcNow) }, 
            tags: new[] { "order:12345", "payment:abc" });
        
        await store.AppendAsync("inventory-stream", 
            new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, 
            tags: new[] { "order:12345", "product:xyz" });

        // Query for all events related to this order (crosses multiple streams/entities)
        var query = Query.FromItems(QueryItem.ByTags("order:12345"));

        // Act
        var result = await store.ReadByQueryAsync(query);

        // Assert - Should return all 3 events across different streams
        Assert.Equal(3, result.Events.Count);
        Assert.All(result.Events, e => Assert.Contains("order:12345", e.Tags));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_MultiEntityConsistency_EnforcesAtomicity()
    {
        // Arrange: Test multi-entity consistency enforcement using DCB
        // This simulates an order fulfillment workflow spanning Order, Payment, and Inventory
        var store = new InMemoryEventStore();
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

        var result = await store.AppendAsync(
            workflowEvents, 
            condition, 
            tags: new[] { orderId, paymentId, productId });

        // Assert - Both events appended atomically
        Assert.Equal(2, result.SequencePositions.Count);

        // Verify query returns both events (cross-entity query)
        var queryResult = await store.ReadByQueryAsync(query);
        Assert.Equal(2, queryResult.Events.Count);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_UsesOpaqueDcbStreamId()
    {
        var store = new InMemoryEventStore();
        var append = await store.AppendAsync(
            new IEvent[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) },
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("order:9001"))),
            metadata: new EventMetadata { TenantId = "tenant-x" },
            tags: new[] { "order:9001" });

        var query = await store.ReadByQueryAsync(Query.All(), fromSequencePosition: append.SequencePositions[0], limit: 1);
        var evt = Assert.Single(query.Events);

        Assert.StartsWith("tenant-x:dcb:", evt.StreamId);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToVersion_LimitsResults()
    {
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events = new[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };
        await store.AppendAsync(streamId, events);

        var result = await store.ReadStreamAsync(streamId, fromVersion: 0, toVersion: 2);

        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);
        Assert.Equal(2, result[1].Version);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToTimestamp_LimitsResults()
    {
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var t1 = new DateTime(2025, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var t2 = new DateTime(2025, 1, 1, 11, 0, 0, DateTimeKind.Utc);
        var t3 = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), t1) }, metadata: new EventMetadata { Timestamp = t1 });
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), t2) }, metadata: new EventMetadata { Timestamp = t2 });
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), t3) }, metadata: new EventMetadata { Timestamp = t3 });

        // Include events with Timestamp <= t2 (v1 and v2)
        var result = await store.ReadStreamAsync(streamId, fromVersion: 0, toTimestamp: t2);

        Assert.Equal(2, result.Count);
        Assert.True(result.All(e => e.Metadata.Timestamp <= t2));
        Assert.All(result, e => Assert.True(e.Metadata.CommitTimestamp > t2));
    }

    [Fact]
    public async Task AppendAsync_SetsCommitTimestamp_WithoutChangingBusinessTime()
    {
        var store = new InMemoryEventStore();
        var business = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        await store.AppendAsync(
            "clock-stream",
            [new TestEvent(Guid.NewGuid(), business)],
            metadata: new EventMetadata { Timestamp = business });

        var stored = Assert.Single(await store.ReadStreamAsync("clock-stream"));
        Assert.Equal(business, stored.Metadata.Timestamp);
        Assert.NotNull(stored.Metadata.CommitTimestamp);
        Assert.True(stored.Metadata.CommitTimestamp > business);

        var asOfBusiness = await store.ReadStreamAsync("clock-stream", toTimestamp: business);
        Assert.Single(asOfBusiness);
    }

    [Fact]
    public async Task ReadStreamEnumerableAsync_YieldsEventsInOrder()
    {
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var events = new[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        };
        await store.AppendAsync(streamId, events);

        var collected = new List<SequencedEvent>();
        await foreach (var evt in store.ReadStreamEnumerableAsync(streamId))
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
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        await store.AppendAsync(streamId, new[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        });

        var collected = new List<SequencedEvent>();
        await foreach (var evt in store.ReadStreamEnumerableAsync(streamId, toVersion: 2))
        {
            collected.Add(evt);
        }

        Assert.Equal(2, collected.Count);
    }

    [Fact]
    public async Task ReadStreamAsync_WithToVersionAndToTimestamp_AppliesBothFilters()
    {
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        var t1 = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var t2 = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var t3 = new DateTime(2025, 1, 2, 12, 0, 0, DateTimeKind.Utc);
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), t1) }, metadata: new EventMetadata { Timestamp = t1 });
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), t2) }, metadata: new EventMetadata { Timestamp = t2 });
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), t3) }, metadata: new EventMetadata { Timestamp = t3 });

        var result = await store.ReadStreamAsync(streamId, fromVersion: 1, toVersion: 2, toTimestamp: t2);

        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Version);
        Assert.Equal(2, result[1].Version);
        Assert.All(result, e => Assert.True(e.Metadata.Timestamp <= t2));
    }

    [Fact]
    public async Task ReadByQueryAsync_WithToSequencePosition_LimitsResults()
    {
        var store = new InMemoryEventStore();
        var streamId = "test-stream";
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, tags: new[] { "order:1" });
        var append2 = await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, tags: new[] { "order:1" });
        await store.AppendAsync(streamId, new[] { new TestEvent(Guid.NewGuid(), DateTime.UtcNow) }, tags: new[] { "order:1" });

        var query = Query.FromItems(QueryItem.ByTags("order:1"));
        var result = await store.ReadByQueryAsync(query, toSequencePosition: append2.SequencePositions[0]);

        Assert.Equal(2, result.Events.Count);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithToTimestamp_LimitsResults()
    {
        var store = new InMemoryEventStore();
        var t1 = new DateTime(2025, 4, 1, 10, 0, 0, DateTimeKind.Utc);
        var t2 = new DateTime(2025, 4, 1, 11, 0, 0, DateTimeKind.Utc);
        var t3 = new DateTime(2025, 4, 1, 12, 0, 0, DateTimeKind.Utc);
        await store.AppendAsync("s1", new[] { new TestEvent(Guid.NewGuid(), t1) }, metadata: new EventMetadata { Timestamp = t1 }, tags: new[] { "order:2" });
        await store.AppendAsync("s1", new[] { new TestEvent(Guid.NewGuid(), t2) }, metadata: new EventMetadata { Timestamp = t2 }, tags: new[] { "order:2" });
        await store.AppendAsync("s1", new[] { new TestEvent(Guid.NewGuid(), t3) }, metadata: new EventMetadata { Timestamp = t3 }, tags: new[] { "order:2" });

        var query = Query.FromItems(QueryItem.ByTags("order:2"));
        var result = await store.ReadByQueryAsync(query, toTimestamp: t2);

        Assert.Equal(2, result.Events.Count);
    }

    [Fact]
    public async Task Read_after_append_is_not_the_same_instance()
    {
        var store = new InMemoryEventStore();
        var evt = new TestEvent(Guid.NewGuid(), DateTime.UtcNow);

        await store.AppendAsync("identity-stream", [evt]);
        var stored = Assert.Single(await store.ReadStreamAsync("identity-stream"));

        Assert.False(ReferenceEquals(evt, stored.Event));
        Assert.Equal(evt.Id, stored.Event.Id);
    }

    [Fact]
    public async Task Mutating_caller_payload_array_after_log_append_does_not_change_store()
    {
        var store = new InMemoryEventStore();
        var payload = new byte[] { 1, 2, 3 };
        var metadata = new byte[] { 9, 8, 7 };
        var envelope = new AppendEvent("tests.inmemory.test-event", payload, metadata);

        await store.AppendAsync("buffer-stream", [envelope]);
        payload[0] = 99;
        metadata[0] = 99;

        var recorded = Assert.Single(await ((IEventLog)store).ReadStreamAsync("buffer-stream"));
        Assert.Equal(new byte[] { 1, 2, 3 }, recorded.Payload.ToArray());
        Assert.Equal(new byte[] { 9, 8, 7 }, recorded.Metadata.ToArray());
    }

    [Fact]
    public async Task Mutating_caller_metadata_after_append_is_not_observable()
    {
        var store = new InMemoryEventStore();
        var metadata = new EventMetadata { UserId = "u-1", Timestamp = DateTime.UtcNow };

        await store.AppendAsync("meta-stream", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)], metadata: metadata);
        metadata.UserId = "mutated";

        var stored = Assert.Single(await store.ReadStreamAsync("meta-stream"));
        Assert.Equal("u-1", stored.Metadata.UserId);
        Assert.NotSame(metadata, stored.Metadata);
        Assert.NotNull(stored.Metadata.CommitTimestamp);
    }

    [Fact]
    public async Task ReadByQueryAsync_WithAliasedTypeFilter_UsesEventTypeAlias()
    {
        var store = new InMemoryEventStore();
        await store.AppendAsync("tenant:Order:1", new IEvent[]
        {
            new AliasedEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow)
        });

        var result = await store.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByType(EventTypeCatalog.TryGetDeclaredName(typeof(AliasedEvent))!)));

        var evt = Assert.Single(result.Events);
        Assert.IsType<AliasedEvent>(evt.Event);

        var byFullName = await store.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByType(typeof(AliasedEvent).FullName!)));
        Assert.Empty(byFullName.Events);
    }

    [Fact]
    public async Task Append_round_trips_schema_version_one_and_two()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(TestEvent), typeof(SchemaV1), typeof(SchemaCurrent)]);
        var store = new InMemoryEventStore(
            session: new EventSession(new LawnDart.EventSourcing.Serialization.JsonEventSerializer(), catalog));

        await store.AppendAsync("stream-v1", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await store.AppendAsync("stream-v2", [new SchemaCurrent(Guid.NewGuid(), DateTime.UtcNow, "Ada", "bio")]);

        var typedV1 = Assert.Single(await store.ReadStreamAsync("stream-v1"));
        Assert.IsType<TestEvent>(typedV1.Event);
        Assert.Equal(1, typedV1.Metadata.SchemaVersion);
        Assert.Equal(1, Assert.Single(await ((IEventLog)store).ReadStreamAsync("stream-v1")).SchemaVersion);

        var typedV2 = Assert.Single(await store.ReadStreamAsync("stream-v2"));
        Assert.IsType<SchemaCurrent>(typedV2.Event);
        Assert.Equal(2, typedV2.Metadata.SchemaVersion);
        Assert.Equal(2, Assert.Single(await ((IEventLog)store).ReadStreamAsync("stream-v2")).SchemaVersion);
    }

    [Fact]
    public async Task Log_v1_row_typed_read_upcasts_to_current()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(SchemaV1), typeof(SchemaCurrent)]);
        var pipeline = EventUpcastPipeline.Materialize(catalog, [typeof(SchemaV1ToCurrent)]);
        var serializer = new LawnDart.EventSourcing.Serialization.JsonEventSerializer();
        var store = new InMemoryEventStore(session: new EventSession(serializer, catalog, pipeline));
        var v1 = new SchemaV1(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var payload = serializer.Serialize(v1, v1.GetType());

        await ((IEventLog)store).AppendAsync(
            "evolved-stream",
            [new AppendEvent("tests.inmemory.schema-evolved", payload, schemaVersion: 1)]);

        var recorded = Assert.Single(await ((IEventLog)store).ReadStreamAsync("evolved-stream"));
        Assert.Equal(1, recorded.SchemaVersion);

        var typed = Assert.Single(await store.ReadStreamAsync("evolved-stream"));
        var current = Assert.IsType<SchemaCurrent>(typed.Event);
        Assert.Equal("Ada", current.Name);
        Assert.Equal("", current.Bio);
        Assert.Equal(1, typed.Metadata.SchemaVersion);
    }

    [Fact]
    public async Task Log_unknown_family_reads_frame_typed_read_fails_closed()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(TestEvent)]);
        var store = new InMemoryEventStore(
            session: new EventSession(new LawnDart.EventSourcing.Serialization.JsonEventSerializer(), catalog));
        var payload = """{"kind":"opaque"}"""u8.ToArray();
        var metadata = """{"UserId":"log"}"""u8.ToArray();

        await ((IEventLog)store).AppendAsync(
            "opaque-stream",
            [new AppendEvent("foreign-family", payload, metadata, schemaVersion: 1)]);

        var recorded = Assert.Single(await ((IEventLog)store).ReadStreamAsync("opaque-stream"));
        Assert.Equal("foreign-family", recorded.EventType);
        Assert.Equal(payload, recorded.Payload.ToArray());

        var ex = await Assert.ThrowsAsync<UnknownEventFamilyException>(
            () => store.ReadStreamAsync("opaque-stream"));
        Assert.Equal("foreign-family", ex.FamilyToken);
    }

    [Fact]
    public async Task Log_schema_version_99_reads_frame_typed_read_is_too_new()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(TestEvent)]);
        var store = new InMemoryEventStore(
            session: new EventSession(new LawnDart.EventSourcing.Serialization.JsonEventSerializer(), catalog));
        var payload = """{"Id":"00000000-0000-0000-0000-000000000000"}"""u8.ToArray();
        var metadata = """{"UserId":"log"}"""u8.ToArray();

        await ((IEventLog)store).AppendAsync(
            "newer-stream",
            [new AppendEvent("tests.inmemory.test-event", payload, metadata, schemaVersion: 99)]);

        var recorded = Assert.Single(await ((IEventLog)store).ReadStreamAsync("newer-stream"));
        Assert.Equal(99, recorded.SchemaVersion);

        var ex = await Assert.ThrowsAsync<EventSchemaTooNewException>(
            () => store.ReadStreamAsync("newer-stream"));
        Assert.Equal("tests.inmemory.test-event", ex.FamilyToken);
        Assert.Equal(99, ex.SchemaVersion);
        Assert.Equal(1, ex.ProcessCurrentVersion);
    }

    [Fact]
    public async Task Log_persists_codec_id_without_a_mime_literal()
    {
        var store = new InMemoryEventStore();
        await ((IEventLog)store).AppendAsync(
            "codec-r2",
            [new AppendEvent("family", new byte[] { 1 }, codecId: EventCodec.MemoryPack)]);

        var recorded = Assert.Single(await ((IEventLog)store).ReadStreamAsync("codec-r2"));
        Assert.Equal(EventCodec.MemoryPack, recorded.CodecId);
        Assert.Equal(typeof(byte), recorded.CodecId.GetType());
        Assert.DoesNotContain("application/vnd.lawndart.memorypack", recorded.CodecId.ToString());
        Assert.DoesNotContain("application/json", recorded.CodecId.ToString());
    }

    [EventTypeName("tests.inmemory.test-event")]
    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
    [EventTypeName("tests.inmemory.another-event")]
    private record AnotherEvent(Guid Id, DateTime Timestamp) : IEvent;
    [EventTypeName("tests.inmemory.alias-event")]
    private record AliasedEvent(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("tests.inmemory.schema-evolved", version: 1)]
    public sealed record SchemaV1(Guid Id, DateTime Timestamp, string Name) : IEvent;

    [EventTypeName("tests.inmemory.schema-evolved", version: 2, current: true)]
    public sealed record SchemaCurrent(Guid Id, DateTime Timestamp, string Name, string Bio) : IEvent;

    public sealed class SchemaV1ToCurrent : IEventUpcaster<SchemaCurrent, SchemaV1>
    {
        public SchemaCurrent Upcast(SchemaV1 source) => new(source.Id, source.Timestamp, source.Name, "");
    }
}

