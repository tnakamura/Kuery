using System;
using System.Threading.Tasks;
using Kuery;

namespace KueryBenchmark
{
    public class DeleteAsync : BenchmarkBase
    {
        private Todo _sqlitePclNetTodo;

        private Todo _kueryTodo;

        public override void IterationSetup()
        {
            base.IterationSetup();

            _sqlitePclNetTodo = new Todo
            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            };
            SQLiteNetPclAsyncConnection.InsertAsync(_sqlitePclNetTodo)
                .GetAwaiter()
                .GetResult();

            _kueryTodo = new Todo

            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            };
            KueryConnection.Insert(_kueryTodo);
        }

        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<int> SQLiteNetPCL() =>
            SQLiteNetPclAsyncConnection.DeleteAsync(_sqlitePclNetTodo);

        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<int> Kuery() =>
            KueryConnection.DeleteAsync(_kueryTodo);
    }
}
