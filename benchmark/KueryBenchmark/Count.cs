using System;
using Kuery;

namespace KueryBenchmark
{
    public class Count : BenchmarkBase
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
        public int SQLiteNetPCL() =>
            SQLiteNetPclConnection.Table<Todo>().Count();

        [BenchmarkDotNet.Attributes.Benchmark]
        public int Kuery() =>
            KueryConnection.Table<Todo>().Count();
    }
}
