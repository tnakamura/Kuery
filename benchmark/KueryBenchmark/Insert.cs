using System;
using Kuery;

namespace KueryBenchmark
{
    public class Insert : BenchmarkBase
    {
        [BenchmarkDotNet.Attributes.Benchmark]
        public int SQLiteNetPCL() =>
            SQLiteNetPclConnection.Insert(new Todo
            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            });

        [BenchmarkDotNet.Attributes.Benchmark]
        public int Kuery() =>
            KueryConnection.Insert(new Todo
            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            });
    }
}
