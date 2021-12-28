using System;
using System.Threading.Tasks;
using Kuery;

namespace KueryBenchmark
{
    public class InsertAsync : BenchmarkBase
    {
        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<int> SQLiteNetPCL() =>
            SQLiteNetPclAsyncConnection.InsertAsync(new Todo
            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            });

        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<int> Kuery() =>
            KueryConnection.InsertAsync(new Todo
            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            });
    }
}
