using System.Globalization;
using Hangfire.Common;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Raft.Monitoring;

/// <summary>
/// Dashboard read API. All data comes from the local node's store, so dashboard pages reflect the
/// locally applied log prefix; on a healthy cluster that is at most a heartbeat behind the leader.
/// </summary>
internal sealed class RaftMonitoringApi(RaftJobStorage storage) : JobStorageMonitor
{
    private RaftStore Store => storage.Cluster.Store;

    public override IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
        var queues = Store.GetQueues(topJobCount: 5);
        // One pass over the fetched leases instead of one scan per queue.
        var fetchedByQueue = Store.GetFetchedCountsByQueue();
        var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);
        foreach (var queue in queues)
        {
            result.Add(new QueueWithTopEnqueuedJobsDto
            {
                Name = queue.Name,
                Length = queue.Length,
                Fetched = fetchedByQueue.GetValueOrDefault(queue.Name),
                FirstJobs = EnqueuedJobs(queue.TopJobIds),
            });
        }

        return result;
    }

    public override IList<ServerDto> Servers()
        => Store.GetServers()
            .Select(s => new ServerDto
            {
                Name = s.Id,
                WorkersCount = s.WorkerCount,
                Queues = s.Queues.ToList(),
                StartedAt = s.StartedAt,
                Heartbeat = s.LastHeartbeat,
            })
            .ToList();

    public override JobDetailsDto? JobDetails(string jobId)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        var job = Store.GetJob(jobId);
        if (job is null) return null;

        return new JobDetailsDto
        {
            Job = TryDeserializeJob(job.InvocationData, out var loadException),
            LoadException = loadException,
            CreatedAt = job.CreatedAt,
            ExpireAt = job.ExpireAt,
            Properties = job.Parameters.ToDictionary(p => p.Key, p => p.Value!),
            History = job.History
                .Select(s => new StateHistoryDto
                {
                    StateName = s.Name,
                    Reason = s.Reason,
                    CreatedAt = s.CreatedAt,
                    Data = ToDictionary(s.Data),
                })
                .Reverse()
                .ToList(),
        };
    }

    public override StatisticsDto GetStatistics()
    {
        var stats = Store.GetStatistics();
        return new StatisticsDto
        {
            Servers = stats.Servers,
            Queues = stats.Queues,
            Enqueued = stats.Enqueued,
            Scheduled = stats.Scheduled,
            Processing = stats.Processing,
            Succeeded = stats.Succeeded,
            Failed = stats.Failed,
            Deleted = stats.Deleted,
            Recurring = stats.Recurring,
            Retries = stats.Retries,
            Awaiting = stats.Awaiting,
        };
    }

    public override JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        return EnqueuedJobs(Store.GetEnqueuedJobIds(queue, from, perPage));
    }

    private JobList<EnqueuedJobDto> EnqueuedJobs(IReadOnlyList<string> jobIds)
    {
        var result = new List<KeyValuePair<string, EnqueuedJobDto>>(jobIds.Count);
        foreach (var jobId in jobIds)
        {
            var job = Store.GetJob(jobId);
            if (job is null) continue;
            var data = StateData(job, EnqueuedState.StateName);
            result.Add(new(jobId, new EnqueuedJobDto
            {
                Job = TryDeserializeJob(job.InvocationData, out _),
                State = job.CurrentState?.Name,
                InEnqueuedState = InState(job, EnqueuedState.StateName),
                EnqueuedAt = ParseDate(data, "EnqueuedAt"),
                StateData = data,
            }));
        }

        return new JobList<EnqueuedJobDto>(result);
    }

    public override JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        var fetched = Store.GetFetchedJobs(queue, from, perPage);
        var result = new List<KeyValuePair<string, FetchedJobDto>>(fetched.Count);
        foreach (var (jobId, fetchedAt) in fetched)
        {
            var job = Store.GetJob(jobId);
            if (job is null) continue;
            result.Add(new(jobId, new FetchedJobDto
            {
                Job = TryDeserializeJob(job.InvocationData, out _),
                State = job.CurrentState?.Name,
                FetchedAt = fetchedAt,
            }));
        }

        return new JobList<FetchedJobDto>(result);
    }

    public override JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        => MapState(ProcessingState.StateName, from, count, ascending: true, (job, data) => new ProcessingJobDto
        {
            Job = TryDeserializeJob(job.InvocationData, out _),
            InProcessingState = InState(job, ProcessingState.StateName),
            ServerId = data.GetValueOrDefault("ServerId"),
            StartedAt = ParseDate(data, "StartedAt") ?? job.CurrentState?.CreatedAt,
            StateData = data,
        });

    public override JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
    {
        // Page from the "schedule" set ordered by score (the planned enqueue time), like the SQL
        // storage: the page then lists next-to-run first. The Scheduled state index would order by
        // state-transition time instead, which is meaningless for this page.
        var ids = Store.GetRangeFromSet("schedule", from, from + count - 1);
        var result = new List<KeyValuePair<string, ScheduledJobDto>>(ids.Count);
        foreach (var id in ids)
        {
            var job = Store.GetJob(id);
            if (job is null) continue;
            var data = StateData(job, ScheduledState.StateName);
            result.Add(new(id, new ScheduledJobDto
            {
                Job = TryDeserializeJob(job.InvocationData, out _),
                InScheduledState = InState(job, ScheduledState.StateName),
                EnqueueAt = ParseDate(data, "EnqueueAt") ?? default,
                ScheduledAt = ParseDate(data, "ScheduledAt") ?? job.CurrentState?.CreatedAt,
                StateData = data,
            }));
        }

        return new JobList<ScheduledJobDto>(result);
    }

    public override JobList<SucceededJobDto> SucceededJobs(int from, int count)
        => MapState(SucceededState.StateName, from, count, ascending: false, (job, data) => new SucceededJobDto
        {
            Job = TryDeserializeJob(job.InvocationData, out _),
            InSucceededState = InState(job, SucceededState.StateName),
            Result = data.GetValueOrDefault("Result"),
            TotalDuration = ParseLong(data, "PerformanceDuration") + ParseLong(data, "Latency"),
            SucceededAt = ParseDate(data, "SucceededAt") ?? job.CurrentState?.CreatedAt,
            StateData = data,
        });

    public override JobList<FailedJobDto> FailedJobs(int from, int count)
        => MapState(FailedState.StateName, from, count, ascending: false, (job, data) => new FailedJobDto
        {
            Job = TryDeserializeJob(job.InvocationData, out _),
            InFailedState = InState(job, FailedState.StateName),
            Reason = job.CurrentState?.Reason,
            ExceptionType = data.GetValueOrDefault("ExceptionType"),
            ExceptionMessage = data.GetValueOrDefault("ExceptionMessage"),
            ExceptionDetails = data.GetValueOrDefault("ExceptionDetails"),
            FailedAt = ParseDate(data, "FailedAt") ?? job.CurrentState?.CreatedAt,
            StateData = data,
        });

    public override JobList<DeletedJobDto> DeletedJobs(int from, int count)
        => MapState(DeletedState.StateName, from, count, ascending: false, (job, data) => new DeletedJobDto
        {
            Job = TryDeserializeJob(job.InvocationData, out _),
            InDeletedState = InState(job, DeletedState.StateName),
            DeletedAt = ParseDate(data, "DeletedAt") ?? job.CurrentState?.CreatedAt,
            StateData = data,
        });

    public override JobList<AwaitingJobDto> AwaitingJobs(int from, int count)
        => MapState(AwaitingState.StateName, from, count, ascending: true, (job, data) => new AwaitingJobDto
        {
            Job = TryDeserializeJob(job.InvocationData, out _),
            AwaitingAt = job.CurrentState?.CreatedAt,
            ParentStateName = data.GetValueOrDefault("NextState") is { } nextState
                ? ParseStateName(nextState)
                : null,
            StateData = data,
        });

    public override long ScheduledCount() => Store.GetSetCount("schedule"); // matches the page source above

    public override long ProcessingCount() => Store.GetStateCount(ProcessingState.StateName);

    public override long FailedCount() => Store.GetStateCount(FailedState.StateName);

    public override long SucceededListCount() => Store.GetStateCount(SucceededState.StateName);

    public override long DeletedListCount() => Store.GetStateCount(DeletedState.StateName);

    public override long AwaitingCount() => Store.GetStateCount(AwaitingState.StateName);

    public override long EnqueuedCount(string queue)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        return Store.GetQueueLength(queue);
    }

    public override long FetchedCount(string queue)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        return Store.GetFetchedCount(queue);
    }

    public override IDictionary<DateTime, long> SucceededByDatesCount() => DailyCounts("succeeded");

    public override IDictionary<DateTime, long> FailedByDatesCount() => DailyCounts("failed");

    public override IDictionary<DateTime, long> DeletedByDatesCount() => DailyCounts("deleted");

    public override IDictionary<DateTime, long> HourlySucceededJobs() => HourlyCounts("succeeded");

    public override IDictionary<DateTime, long> HourlyFailedJobs() => HourlyCounts("failed");

    public override IDictionary<DateTime, long> HourlyDeletedJobs() => HourlyCounts("deleted");

    private JobList<TDto> MapState<TDto>(string stateName, int from, int count, bool ascending, Func<JobSnapshot, Dictionary<string, string>, TDto> map)
    {
        var jobs = Store.GetJobsByState(stateName, from, count, ascending);
        var result = new List<KeyValuePair<string, TDto>>(jobs.Count);
        foreach (var job in jobs)
        {
            result.Add(new(job.Id, map(job, StateData(job, stateName))));
        }

        return new JobList<TDto>(result);
    }

    private Dictionary<DateTime, long> DailyCounts(string type)
    {
        // Counter keys are written by Hangfire core with invariant formatting; matching must not
        // depend on the dashboard thread's culture (calendars, digit substitution).
        var today = DateTime.UtcNow.Date;
        var result = new Dictionary<DateTime, long>();
        for (var i = 0; i < 7; i++)
        {
            var date = today.AddDays(-i);
            result[date] = Store.GetCounter($"stats:{type}:{date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}");
        }

        return result;
    }

    private Dictionary<DateTime, long> HourlyCounts(string type)
    {
        var now = DateTime.UtcNow;
        var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
        var result = new Dictionary<DateTime, long>();
        for (var i = 0; i < 24; i++)
        {
            var slot = hour.AddHours(-i);
            result[slot] = Store.GetCounter($"stats:{type}:{slot.ToString("yyyy-MM-dd-HH", CultureInfo.InvariantCulture)}");
        }

        return result;
    }

    /// <summary>State data of the current state when it matches, otherwise of the latest history entry with that name.</summary>
    private static Dictionary<string, string> StateData(JobSnapshot job, string stateName)
    {
        var state = job.CurrentState?.Name == stateName
            ? job.CurrentState
            : job.History.LastOrDefault(s => s.Name == stateName);
        return state is null ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) : ToDictionary(state.Data);
    }

    private static Dictionary<string, string> ToDictionary(IReadOnlyList<KeyValuePair<string, string?>> pairs)
    {
        var result = new Dictionary<string, string>(pairs.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in pairs) result[key] = value!;
        return result;
    }

    private static bool InState(JobSnapshot job, string stateName) => job.CurrentState?.Name == stateName;

    private static Job? TryDeserializeJob(string payload, out JobLoadException? exception)
    {
        try
        {
            exception = null;
            return InvocationData.DeserializePayload(payload).DeserializeJob();
        }
        catch (JobLoadException ex)
        {
            exception = ex;
            return null;
        }
    }

    private static DateTime? ParseDate(Dictionary<string, string> data, string key)
        => data.TryGetValue(key, out var value) && value is not null ? JobHelper.DeserializeNullableDateTime(value) : null;

    private static long ParseLong(Dictionary<string, string> data, string key)
        => data.TryGetValue(key, out var value) && long.TryParse(value, out var result) ? result : 0;

    /// <summary>
    /// Extracts the state name from an awaiting continuation's serialized NextState JSON.
    /// Input:  {"$type":"...","Name":"Enqueued",...} -> "Enqueued"; unparseable input -> null.
    /// </summary>
    private static string? ParseStateName(string nextStateJson)
    {
        const string marker = "\"Name\":\"";
        var start = nextStateJson.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (start < 0) return null;
        start += marker.Length;
        var end = nextStateJson.IndexOf('"', start);
        return end > start ? nextStateJson[start..end] : null;
    }
}
