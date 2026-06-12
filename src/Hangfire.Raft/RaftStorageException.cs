namespace Hangfire.Raft;

/// <summary>
/// Thrown when a storage operation cannot be completed, typically because the cluster has no
/// leader or quorum within the configured submit timeout. Hangfire components treat storage
/// exceptions as transient and retry.
/// </summary>
public sealed class RaftStorageException : Exception
{
    /// <summary>Creates the exception with a description of the failed operation.</summary>
    public RaftStorageException(string message) : base(message)
    {
    }

    /// <summary>Creates the exception wrapping the underlying cluster or I/O failure.</summary>
    public RaftStorageException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
