using System;
using System.Threading.Tasks;
using Kuery;

namespace KueryBenchmark
{
    public class UpdateAsync : BenchmarkBase
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
            _sqlitePclNetTodo.Name = "Hoge";
            _sqlitePclNetTodo.Description = "Fuga";
            _sqlitePclNetTodo.IsDone = true;
            _sqlitePclNetTodo.UpdatedAt = DateTimeOffset.Now;

            _kueryTodo = new Todo

            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            };
            KueryConnection.Insert(_kueryTodo);
            _kueryTodo.Name = "Hoge";
            _kueryTodo.Description = "Fuga";
            _kueryTodo.IsDone = true;
            _kueryTodo.UpdatedAt = DateTimeOffset.Now;
        }

        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<int> SQLiteNetPCL() =>
            SQLiteNetPclAsyncConnection.UpdateAsync(_sqlitePclNetTodo);

        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<int> Kuery() =>
            KueryConnection.UpdateAsync(_kueryTodo);
    }
}
