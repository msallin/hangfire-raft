using static Hangfire.Raft.Commands.BinaryFormat;

namespace Hangfire.Raft.State;

internal sealed partial class RaftStore
{
    private const byte SnapshotVersion = 1;

    /// <summary>
    /// Serializes the full store state under the store lock. During log compaction this runs on the
    /// snapshot builder's shadow store, so the live store never blocks on snapshot writing.
    /// </summary>
    public void WriteSnapshot(BinaryWriter w)
    {
        lock (_sync)
        {
            w.Write(SnapshotVersion);

            w.Write7BitEncodedInt(_jobs.Count);
            foreach (var (id, job) in _jobs)
            {
                w.Write(id);
                w.Write(job.InvocationData);
                w.Write(job.CreatedAt.Ticks);
                WriteNullable(w, job.ExpireAt);
                WritePairs(w, job.Parameters.ToArray());
                w.Write7BitEncodedInt(job.History.Count);
                foreach (var state in job.History) WriteState(w, state);
                w.Write(job.CurrentState is not null);
                if (job.CurrentState is not null) WriteState(w, job.CurrentState);
            }

            w.Write7BitEncodedInt(_queues.Count);
            foreach (var (name, queue) in _queues)
            {
                w.Write(name);
                w.Write7BitEncodedInt(queue.Count);
                foreach (var jobId in queue) w.Write(jobId);
            }

            w.Write7BitEncodedInt(_fetched.Count);
            foreach (var (token, fetched) in _fetched)
            {
                WriteGuid(w, token);
                w.Write(fetched.JobId);
                w.Write(fetched.Queue);
                w.Write(fetched.FetchedAt.Ticks);
            }

            w.Write7BitEncodedInt(_sets.Count);
            foreach (var (key, set) in _sets)
            {
                w.Write(key);
                WriteNullable(w, set.ExpireAt);
                w.Write7BitEncodedInt(set.Sorted.Count);
                foreach (var item in set.Sorted)
                {
                    w.Write(item.Value);
                    w.Write(item.Score);
                }
            }

            w.Write7BitEncodedInt(_lists.Count);
            foreach (var (key, list) in _lists)
            {
                w.Write(key);
                WriteNullable(w, list.ExpireAt);
                WriteStrings(w, list.Items);
            }

            w.Write7BitEncodedInt(_hashes.Count);
            foreach (var (key, hash) in _hashes)
            {
                w.Write(key);
                WriteNullable(w, hash.ExpireAt);
                WritePairs(w, hash.Fields.ToArray());
            }

            w.Write7BitEncodedInt(_counters.Count);
            foreach (var (key, counter) in _counters)
            {
                w.Write(key);
                w.Write(counter.Value);
                WriteNullable(w, counter.ExpireAt);
            }

            w.Write7BitEncodedInt(_servers.Count);
            foreach (var (id, server) in _servers)
            {
                w.Write(id);
                w.Write(server.WorkerCount);
                WriteStrings(w, server.Queues);
                w.Write(server.StartedAt.Ticks);
                w.Write(server.LastHeartbeat.Ticks);
            }

            w.Write7BitEncodedInt(_locks.Count);
            foreach (var (resource, lockEntry) in _locks)
            {
                w.Write(resource);
                WriteGuid(w, lockEntry.Owner);
                w.Write(lockEntry.ExpiresAt.Ticks);
            }
        }
    }

    /// <summary>
    /// Replaces the entire store content in place, so existing references stay valid. The snapshot is
    /// parsed into a throwaway store first, so a corrupt or truncated stream throws before the live
    /// store is touched and readers never observe a half-loaded store. Only a fully parsed snapshot is
    /// swapped in, and the swap runs under the store lock and cannot throw.
    /// </summary>
    public void LoadSnapshot(BinaryReader r)
    {
        var loaded = new RaftStore();
        loaded.ReadSnapshot(r);

        lock (_sync)
        {
            ReplaceContentsWith(loaded);
        }
    }

    /// <summary>
    /// Deserializes a snapshot into this store, which is assumed empty (a throwaway instance created by
    /// <see cref="LoadSnapshot"/>). A malformed stream throws here, before any caller can observe the
    /// instance, which is what keeps the load all-or-nothing.
    /// </summary>
    private void ReadSnapshot(BinaryReader r)
    {
        var version = r.ReadByte();
        if (version != SnapshotVersion) throw new NotSupportedException($"Unknown snapshot version {version}.");

        for (var count = ReadCount(r); count > 0; count--)
        {
            var id = r.ReadString();
            var job = new JobEntry
            {
                Id = id,
                InvocationData = r.ReadString(),
                CreatedAt = ReadDate(r),
                ExpireAt = ReadNullableDate(r),
            };
            foreach (var (key, value) in ReadPairs(r)) job.Parameters[key] = value;
            for (var states = ReadCount(r); states > 0; states--) job.History.Add(ReadState(r));
            if (r.ReadBoolean())
            {
                job.CurrentState = ReadState(r);
                job.StateChangedAt = job.CurrentState.CreatedAt;
            }
            _jobs[id] = job;
            AddToStateIndex(job);
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var queue = Queue(r.ReadString());
            for (var items = ReadCount(r); items > 0; items--) queue.AddLast(r.ReadString());
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var token = ReadGuid(r);
            _fetched[token] = new FetchedEntry { JobId = r.ReadString(), Queue = r.ReadString(), FetchedAt = ReadDate(r) };
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var key = r.ReadString();
            var set = new SetEntry { ExpireAt = ReadNullableDate(r) };
            _sets[key] = set;
            for (var items = ReadCount(r); items > 0; items--)
            {
                var value = r.ReadString();
                var score = r.ReadDouble();
                set.Scores[value] = score;
                set.Sorted.Add(new SetItem(score, value));
            }
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var key = r.ReadString();
            var list = new ListEntry { ExpireAt = ReadNullableDate(r) };
            _lists[key] = list;
            list.Items.AddRange(ReadStrings(r));
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var key = r.ReadString();
            var hash = new HashEntry { ExpireAt = ReadNullableDate(r) };
            _hashes[key] = hash;
            foreach (var (field, value) in ReadPairs(r)) hash.Fields[field] = value;
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var key = r.ReadString();
            _counters[key] = new CounterEntry { Value = r.ReadInt64(), ExpireAt = ReadNullableDate(r) };
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var id = r.ReadString();
            _servers[id] = new ServerEntry
            {
                WorkerCount = r.ReadInt32(),
                Queues = ReadStrings(r),
                StartedAt = ReadDate(r),
                LastHeartbeat = ReadDate(r),
            };
        }

        for (var count = ReadCount(r); count > 0; count--)
        {
            var resource = r.ReadString();
            _locks[resource] = new LockEntry { Owner = ReadGuid(r), ExpiresAt = ReadDate(r) };
        }
    }

    /// <summary>
    /// Swaps another store's contents into this one under the caller-held lock. A pure in-memory copy
    /// that cannot throw, so from a reader's perspective the snapshot load is atomic. The source is a
    /// throwaway instance, so handing over its collection contents (including the freshly built state
    /// index, which references the same job entries) is safe.
    /// </summary>
    private void ReplaceContentsWith(RaftStore other)
    {
        Replace(_jobs, other._jobs);
        Replace(_queues, other._queues);
        Replace(_fetched, other._fetched);
        Replace(_sets, other._sets);
        Replace(_lists, other._lists);
        Replace(_hashes, other._hashes);
        Replace(_counters, other._counters);
        Replace(_servers, other._servers);
        Replace(_locks, other._locks);
        Replace(_jobsByState, other._jobsByState);

        static void Replace<TKey, TValue>(Dictionary<TKey, TValue> target, Dictionary<TKey, TValue> source)
            where TKey : notnull
        {
            target.Clear();
            foreach (var (key, value) in source) target[key] = value;
        }
    }
}
