using System;
using System.IO;
using BenchmarkDotNet.Running;

namespace Kuery.Benchmark
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            BenchmarkRunner.Run<Insert>();
        }
    }

    [SQLite.Table("todo")]
    public class Todo
    {
        [SQLite.PrimaryKey]
        [SQLite.Column("id")]
        [SQLite.AutoIncrement]
        public int Id { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("name")]
        public string Name { get; set; }

        [SQLite.Column("description")]
        public string Description { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("done")]
        public bool IsDone { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("created_at")]
        public DateTimeOffset CreatedAt { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("updated_at")]
        public DateTimeOffset UpdatedAt { get; set; }
    }

    public class Insert
    {
        private SQLite.SQLiteConnection _sqliteNetPcl;

        [BenchmarkDotNet.Attributes.GlobalSetup]
        public void GlobalSetup()
        {
            _sqliteNetPcl = new SQLite.SQLiteConnection(
                new SQLite.SQLiteConnectionString(
                    Path.Combine(
                        AppContext.BaseDirectory,
                        "sqlite-net-pcl.sqlite3")));
        }

        [BenchmarkDotNet.Attributes.IterationSetup]
        public void IterationSetup()
        {
            _sqliteNetPcl.CreateTable<Todo>();
        }

        [BenchmarkDotNet.Attributes.IterationCleanup]
        public void IterationCleanup()
        {
            _sqliteNetPcl.DropTable<Todo>();
        }

        [BenchmarkDotNet.Attributes.Benchmark]
        public int SQLiteNetPCL()
        {
            return _sqliteNetPcl.Insert(new Todo
            {
                Name = "Foo",
                Description = "Bar",
                IsDone = false,
                CreatedAt = DateTimeOffset.Now,
                UpdatedAt = DateTimeOffset.Now,
            });
        }
    }
}
