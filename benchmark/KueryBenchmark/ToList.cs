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
                SQLiteNetPclConnection.Insert(new Todo
                {
                    Name = $"Foo{i}",
                    Description = $"Bar{i}",
                    IsDone = false,
                    CreatedAt = DateTimeOffset.Now,
                    UpdatedAt = DateTimeOffset.Now,
                });

                KueryConnection.Insert(new Todo
                {
                    Name = $"Foo{i}",
                    Description = $"Bar{i}",
                    IsDone = false,
                    CreatedAt = DateTimeOffset.Now,
                    UpdatedAt = DateTimeOffset.Now,
                });

                SqliteConnection.Insert(new Todo
                {
                    Name = $"Foo{i}",
                    Description = $"Bar{i}",
                    IsDone = false,
                    CreatedAt = DateTimeOffset.Now,
                    UpdatedAt = DateTimeOffset.Now,
                });
            }
        }

        public override void IterationCleanup()
        {
            base.IterationCleanup();

        }

        [BenchmarkDotNet.Attributes.Benchmark]
        public List<Todo> SQLiteNetPCL() =>
            SQLiteNetPclConnection.Table<Todo>().ToList();

        [BenchmarkDotNet.Attributes.Benchmark]
        public List<Todo> Kuery() =>
            KueryConnection.Table<Todo>().ToList();

        [BenchmarkDotNet.Attributes.Benchmark]
        public List<Todo> AdoDotNet()
        {
            using (var command = KueryConnection.CreateCommand())
            {
                command.CommandText = @"select * from todo";
                using (System.Data.IDataReader reader = command.ExecuteReader())
                {
                    var result = new List<Todo>();
                    while (reader.Read())
                    {
                        var todo = new Todo();
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var name = reader.GetName(i);
                            if (name == "id")
                                todo.Id = reader.GetInt32(i);
                            else if (name == "name")
                                todo.Name = reader.GetString(i);
                            else if (name == "description")
                                todo.Description = reader.GetString(i);
                            else if (name == "done")
                                todo.IsDone = reader.GetBoolean(i);
                            else if (name == "created_at")
                            {
                                var value = reader.GetValue(i);
                                if (value is string s && DateTimeOffset.TryParse(s, out var createdAt))
                                {
                                    todo.CreatedAt = createdAt;
                                }
                                else if (value is DateTimeOffset dto)
                                {
                                    todo.CreatedAt = dto;
                                }
                            }
                            else if (name == "updated_at")
                            {
                                var value = reader.GetValue(i);
                                if (value is string s && DateTimeOffset.TryParse(s, out var updatedAt))
                                {
                                    todo.UpdatedAt = updatedAt;
                                }
                                else if (value is DateTimeOffset dto)
                                {
                                    todo.UpdatedAt = dto;
                                }
                            }
                        }
                        result.Add(todo);
                    }
                    return result;
                }
            }
        }
    }
}
