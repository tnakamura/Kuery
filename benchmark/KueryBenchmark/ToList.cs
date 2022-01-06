using System;
using System.Collections.Generic;
using Kuery;

namespace KueryBenchmark
{
    public class ToList : BenchmarkBase
    {
        public override void IterationSetup()
        {
            base.IterationSetup();

            for (var i = 0; i < 10; i++)
            {
                var sqlitePclNetTodo = new Todo
                {
                    Name = $"Foo{i}",
                    Description = $"Bar{i}",
                    IsDone = false,
                    CreatedAt = DateTimeOffset.Now,
                    UpdatedAt = DateTimeOffset.Now,
                };
                SQLiteNetPclConnection.Insert(sqlitePclNetTodo);

                var kueryTodo = new Todo
                {
                    Name = $"Foo{i}",
                    Description = $"Bar{i}",
                    IsDone = false,
                    CreatedAt = DateTimeOffset.Now,
                    UpdatedAt = DateTimeOffset.Now,
                };
                KueryConnection.Insert(kueryTodo);
            }
        }

        [BenchmarkDotNet.Attributes.Benchmark]
        public List<Todo> SQLiteNetPCL() =>
            SQLiteNetPclConnection.Table<Todo>().ToList();

        [BenchmarkDotNet.Attributes.Benchmark]
        public List<Todo> Kuery() =>
            KueryConnection.Table<Todo>().ToList();
    }
}
