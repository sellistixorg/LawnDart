using LawnDart;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;

namespace LawnDart.Demo.Academy.Projections;

/// <summary>
/// Per-student enrollment history — a read model of all courses a student
/// has enrolled in, their payment status, and whether a confirmation was sent.
/// </summary>
public class StudentEnrollmentRecord
{
    public Guid StudentId { get; set; }
    public string StudentName { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public List<EnrollmentHistoryEntry> Enrollments { get; set; } = [];
}

public class EnrollmentHistoryEntry
{
    public Guid CourseId { get; set; }
    public bool PaymentProcessed { get; set; }
    public decimal AmountPaid { get; set; }
    public bool ConfirmationSent { get; set; }
    public bool Cancelled { get; set; }
    public DateTimeOffset EnrolledAt { get; set; }
}

/// <summary>
/// Projects student-related events into per-student enrollment transcripts.
/// </summary>
public class StudentTranscriptProjector
{
    private readonly Dictionary<Guid, StudentEnrollmentRecord> _records = [];
    private readonly Lock _lock = new();

    public void Apply(IEvent @event)
    {
        lock (_lock)
        {
            switch (@event)
            {
                case StudentRegistered e:
                    _records[e.StudentId] = new StudentEnrollmentRecord
                    {
                        StudentId = e.StudentId,
                        StudentName = e.Name,
                        Email = e.Email,
                    };
                    break;

                case StudentPaymentProcessed e:
                    if (_records.TryGetValue(e.StudentId, out var payRec))
                    {
                        var entry = GetOrCreateEntry(payRec, e.CourseId);
                        entry.PaymentProcessed = true;
                        entry.AmountPaid = e.Amount;
                    }
                    break;

                case StudentEnrolled e:
                    if (_records.TryGetValue(e.StudentId, out var enrRec))
                    {
                        var entry = GetOrCreateEntry(enrRec, e.CourseId);
                        entry.EnrolledAt = DateTimeOffset.UtcNow;
                    }
                    break;

                case RegistrationConfirmationSent e:
                    if (_records.TryGetValue(e.StudentId, out var confRec))
                    {
                        var entry = GetOrCreateEntry(confRec, e.CourseId);
                        entry.ConfirmationSent = true;
                    }
                    break;

                case RegistrationCancelled e:
                    if (_records.TryGetValue(e.StudentId, out var canRec))
                    {
                        var entry = GetOrCreateEntry(canRec, e.CourseId);
                        entry.Cancelled = true;
                    }
                    break;
            }
        }
    }

    public StudentEnrollmentRecord? Get(Guid studentId)
    {
        lock (_lock)
            return _records.TryGetValue(studentId, out var r) ? r : null;
    }

    public IReadOnlyList<StudentEnrollmentRecord> GetAll()
    {
        lock (_lock)
            return [.. _records.Values];
    }

    private static EnrollmentHistoryEntry GetOrCreateEntry(StudentEnrollmentRecord record, Guid courseId)
    {
        var entry = record.Enrollments.FirstOrDefault(e => e.CourseId == courseId);
        if (entry is null)
        {
            entry = new EnrollmentHistoryEntry { CourseId = courseId };
            record.Enrollments.Add(entry);
        }
        return entry;
    }
}
