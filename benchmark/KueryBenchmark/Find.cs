using System;
using Kuery;

namespace KueryBenchmark
{
    public class Find : BenchmarkBase
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
            SQLiteNetPclConnection.Insert(_sqlitePclNetTodo);

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
        public Todo SQLiteNetPCL() =>
            SQLiteNetPclConnection.Find<Todo>(_sqlitePclNetTodo.Id);

        [BenchmarkDotNet.Attributes.Benchmark]
        public Todo Kuery() =>
            KueryConnection.Find<Todo>(_kueryTodo.Id);
    }
}
