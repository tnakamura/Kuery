using System;
using System.Collections.Generic;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    /// <summary>Stores compiled query plans in a thread-safe, bounded least-recently-used cache.</summary>
    internal static class CompiledQueryCache
    {
        const int Capacity = 256;
        static readonly object Gate = new object();
        static readonly Dictionary<string, LinkedListNode<KeyValuePair<string, CompiledQuery>>> Entries = new Dictionary<string, LinkedListNode<KeyValuePair<string, CompiledQuery>>>();
        static readonly LinkedList<KeyValuePair<string, CompiledQuery>> Recency = new LinkedList<KeyValuePair<string, CompiledQuery>>();
        /// <summary>Gets the number of plans currently stored in the cache.</summary>
        internal static int Count { get { lock (Gate) return Entries.Count; } }

        /// <summary>Removes all compiled query plans from the cache.</summary>
        internal static void Clear()
        {
            lock (Gate) { Entries.Clear(); Recency.Clear(); }
        }

        /// <summary>Returns a cached plan or compiles and caches one while holding the cache lock.</summary>
        /// <param name="key">The structural cache key.</param>
        /// <param name="compile">The delegate that compiles the plan on a cache miss.</param>
        /// <returns>The existing or newly compiled plan.</returns>
        /// <remarks>A cache hit marks the plan as recently used. Adding a plan may evict the least recently used entry.</remarks>
        internal static CompiledQuery GetOrAdd(string key, Func<CompiledQuery> compile)
        {
            lock (Gate)
            {
                if (Entries.TryGetValue(key, out var entry))
                { Recency.Remove(entry); Recency.AddFirst(entry); return entry.Value.Value; }
                var plan = compile();
                var node = Recency.AddFirst(new KeyValuePair<string, CompiledQuery>(key, plan));
                Entries.Add(key, node);
                if (Entries.Count > Capacity) { Entries.Remove(Recency.Last.Value.Key); Recency.RemoveLast(); }
                return plan;
            }
        }

    }
}
