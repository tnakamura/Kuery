using System;
using System.Diagnostics;
using System.Linq;
using Kuery;
using Microsoft.Data.Sqlite;

namespace QueryPipelineProbe
{
    // Uses only the common public API, so the same source can measure old and new library revisions.
    internal static class Program
    {
        static void Main()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            connection.Open();
            connection.Execute("create table probe_row (Id integer primary key, ParentId integer, Name text)");
            connection.Execute("create table probe_parent (Id integer primary key, Name text)");
            for (int i = 1; i <= 1000; i++)
            {
                connection.Insert(new ProbeRow { Id = i, ParentId = i / 2, Name = "Item" + i });
                connection.Insert(new ProbeParent { Id = i, Name = "Parent" + i });
            }
            var rows = connection.Query<ProbeRow>();
            var parents = connection.Query<ProbeParent>();
            Action[] cases =
            {
                () => rows.Where(x => x.Id > 990).ToList(),
                () => rows.Where(x => x.Id > 990).Select(x => new { x.Id, x.Name }).ToList(),
                () => rows.Where(x => x.Id > 990).Join(parents, x => x.ParentId, x => x.Id, (child, parent) => new { child.Id, parent.Name }).ToList(),
            };
            var names = new[] { "entity", "projection", "join" };
            for (int i = 0; i < cases.Length; i++)
            {
                var coldAllocated = GC.GetAllocatedBytesForCurrentThread();
                var cold = Stopwatch.StartNew();
                cases[i]();
                cold.Stop();
                coldAllocated = GC.GetAllocatedBytesForCurrentThread() - coldAllocated;
                for (int j = 0; j < 100; j++) cases[i]();
                const int count = 3000;
                GC.Collect();
                var allocated = GC.GetAllocatedBytesForCurrentThread();
                var watch = Stopwatch.StartNew();
                for (int j = 0; j < count; j++) cases[i]();
                watch.Stop();
                allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;
                Console.WriteLine($"{names[i]}: first={cold.Elapsed.TotalMilliseconds:F3}ms/{coldAllocated}B warm={watch.Elapsed.TotalMicroseconds / count:F2}us/{allocated / count}B");
            }
        }
    }

    [Table("probe_row")]
    public class ProbeRow
    {
        [PrimaryKey]
        public int Id { get; set; }
        public int ParentId { get; set; }
        public string Name { get; set; }
    }

    [Table("probe_parent")]
    public class ProbeParent
    {
        [PrimaryKey]
        public int Id { get; set; }
        public string Name { get; set; }
    }
}
