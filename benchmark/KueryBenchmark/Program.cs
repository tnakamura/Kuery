using System;
using System.Data.Common;
using System.IO;
using BenchmarkDotNet.Running;
using Kuery;

namespace KueryBenchmark
{
    internal class Program
    {
        static void Main(string[] args) =>
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }

    [SQLite.Table("todo")]
    [Table("todo")]
    public class Todo
    {
        [SQLite.PrimaryKey]
        [SQLite.Column("id")]
        [SQLite.AutoIncrement]
        [PrimaryKey]
        [Column("id")]
        [AutoIncrement]
        public int Id { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("name")]
        [NotNull]
        [Column("name")]
        public string Name { get; set; }

        [SQLite.Column("description")]
        [Column("description")]
        public string Description { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("done")]
        [NotNull]
        [Column("done")]
        public bool IsDone { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("created_at")]
        [NotNull]
        [Column("created_at")]
        public DateTimeOffset CreatedAt { get; set; }

        [SQLite.NotNull]
        [SQLite.Column("updated_at")]
        [NotNull]
        [Column("updated_at")]
        public DateTimeOffset UpdatedAt { get; set; }
    }

    internal static class SqliteConnectionExtensions
    {
        public static int CreateTodoTable(this DbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    @"create table if not exists todo (
                        id integer primary key autoincrement,
                        name text not null,
                        description text null,
                        done boolean not null,
                        created_at datetimeoffset not null,
                        updated_at datetimeoffset not null
                      );";
                return command.ExecuteNonQuery();
            }
        }

        public static int DropTodoTable(this DbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"drop table if exists todo;";
                return command.ExecuteNonQuery();
            }
        }
    }

    public abstract class BenchmarkBase
    {
        protected SQLite.SQLiteConnection SQLiteNetPclConnection { get; private set; }

        protected SQLite.SQLiteAsyncConnection SQLiteNetPclAsyncConnection { get; private set; }

        protected Microsoft.Data.Sqlite.SqliteConnection KueryConnection { get; private set; }

        [BenchmarkDotNet.Attributes.GlobalSetup]
        public virtual void GlobalSetup()
        {
            SQLiteNetPclConnection = new SQLite.SQLiteConnection(
                new SQLite.SQLiteConnectionString(
                    Path.Combine(
                        AppContext.BaseDirectory,
                        "sqlite-net-pcl.sqlite3")));

            SQLiteNetPclAsyncConnection = new SQLite.SQLiteAsyncConnection(
                new SQLite.SQLiteConnectionString(
                    Path.Combine(
                        AppContext.BaseDirectory,
                        "sqlite-net-pcl.async.sqlite3")));

            KueryConnection = new Microsoft.Data.Sqlite.SqliteConnection(
                new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder
                {
                    DataSource = Path.Combine(
                        AppContext.BaseDirectory,
                        "kuery.sqlitee"),
                }.ToString());
            KueryConnection.Open();
        }

        [BenchmarkDotNet.Attributes.GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            KueryConnection?.Close();
        }

        [BenchmarkDotNet.Attributes.IterationSetup]
        public virtual void IterationSetup()
        {
            SQLiteNetPclConnection.CreateTable<Todo>();

            SQLiteNetPclAsyncConnection.CreateTableAsync<Todo>()
                .GetAwaiter().GetResult();

            KueryConnection.CreateTodoTable();
        }

        [BenchmarkDotNet.Attributes.IterationCleanup]
        public virtual void IterationCleanup()
        {
            SQLiteNetPclConnection.DropTable<Todo>();

            SQLiteNetPclAsyncConnection.DropTableAsync<Todo>()
                .GetAwaiter().GetResult();

            KueryConnection.DropTodoTable();
        }
    }
}
