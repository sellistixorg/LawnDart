using LawnDart;
using LawnDart.EventStore;

namespace Library.Dcb.Domain;

[EventTypeName("book-added")]
public sealed record BookAdded(
    Guid Id,
    DateTime Timestamp,
    Guid BookId,
    string Title,
    string Isbn) : IEvent;

[EventTypeName("book-borrowed")]
public sealed record BookBorrowed(
    Guid Id,
    DateTime Timestamp,
    Guid BookId,
    Guid MemberId,
    string MemberName) : IEvent;

[EventTypeName("book-returned")]
public sealed record BookReturned(
    Guid Id,
    DateTime Timestamp,
    Guid BookId,
    Guid MemberId) : IEvent;
