using System;
using System.Collections.Generic;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal static class CompiledQueryCache
    {
        const int Capacity = 256;
        static readonly object Gate = new object();
        static readonly Dictionary<string, LinkedListNode<KeyValuePair<string, CompiledQuery>>> Entries = new Dictionary<string, LinkedListNode<KeyValuePair<string, CompiledQuery>>>();
        static readonly LinkedList<KeyValuePair<string, CompiledQuery>> Recency = new LinkedList<KeyValuePair<string, CompiledQuery>>();
        internal static int Count { get { lock (Gate) return Entries.Count; } }

        internal static void Clear()
        {
            lock (Gate) { Entries.Clear(); Recency.Clear(); }
        }

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
