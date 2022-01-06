using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using Kuery;

namespace KueryBenchmark
{
    public class ToListAsync : BenchmarkBase
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
                SQLiteNetPclAsyncConnection
                    .InsertAsync(sqlitePclNetTodo)
                    .GetAwaiter()
                    .GetResult();

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
        public Task<List<Todo>> SQLiteNetPCL() =>
            SQLiteNetPclAsyncConnection.Table<Todo>().ToListAsync();

        [BenchmarkDotNet.Attributes.Benchmark]
        public Task<List<Todo>> Kuery() =>
            KueryConnection.Table<Todo>().ToListAsync();
    }
}
