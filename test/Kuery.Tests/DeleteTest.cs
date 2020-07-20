using System;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests
{
    public class DeleteTest : IClassFixture<SqlServerFixture>
    {
        readonly SqlServerFixture fixture;

        public DeleteTest(SqlServerFixture fixture)
        {
            this.fixture = fixture;
        }

        class TestTable
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }
            public int Datum { get; set; }
            public string Test { get; set; }
        }

        const int Count = 100;

        DbConnection CreateDb()
        {
            var connection = fixture.OpenNewConnection();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
                    if object_id (N'TestTable') is not null
                        drop table TestTable;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
                    if object_id (N'TestTable') is null
                        create table TestTable (
                            Id integer identity(1,1) primary key not null,
                            Datum integer null,
                            Test nvarchar(64) null
                        );";
                cmd.ExecuteNonQuery();
            }

            var items = from i in Enumerable.Range(0, Count)
                        select new TestTable { Datum = 1000 + i, Test = "Hello World" };
            connection.InsertAll(items);
            return connection;
        }

        [Fact]
        public void DeleteEntityOne()
        {
            using var db = CreateDb();

            var r = db.Delete(db.Get<TestTable>(1));

            Assert.Equal(1, r);
            Assert.Equal(Count - 1, db.Table<TestTable>().Count());
        }

        [Fact]
        public void DeletePKOne()
        {
            using var db = CreateDb();

            var r = db.Delete<TestTable>(1);

            Assert.Equal(1, r);
            Assert.Equal(Count - 1, db.Table<TestTable>().Count());
        }

        [Fact]
        public void DeletePKNone()
        {
            using var db = CreateDb();

            var r = db.Delete<TestTable>(348597);

            Assert.Equal(0, r);
            Assert.Equal(Count, db.Table<TestTable>().Count());
        }

        [Fact]
        public void DeleteWithPredicate()
        {
            using var db = CreateDb();

            var r = db.Table<TestTable>().Delete(p => p.Test == "Hello World");

            Assert.Equal(Count, r);
            Assert.Equal(0, db.Table<TestTable>().Count());
        }

        [Fact]
        public void DeleteWithPredicateHalf()
        {
            using var db = CreateDb();
            db.Insert(new TestTable() { Datum = 1, Test = "Hello World 2" });

            var r = db.Table<TestTable>().Delete(p => p.Test == "Hello World");

            Assert.Equal(Count, r);
            Assert.Equal(1, db.Table<TestTable>().Count());
        }

        [Fact]
        public void DeleteWithWherePredicate()
        {
            using var db = CreateDb();

            var r = db.Table<TestTable>().Where(p => p.Test == "Hello World").Delete();

            Assert.Equal(Count, r);
            Assert.Equal(0, db.Table<TestTable>().Count());
        }

        [Fact]
        public void DeleteWithoutPredicate()
        {
            using var db = CreateDb();

            Assert.Throws<InvalidOperationException>(() =>
            {
                var r = db.Table<TestTable>().Delete();
            });
        }

        [Fact]
        public void DeleteWithTake()
        {
            using var db = CreateDb();

            Assert.Throws<InvalidOperationException>(() =>
            {
                var r = db.Table<TestTable>().Where(p => p.Test == "Hello World").Take(2).Delete();
            });
        }

        [Fact]
        public void DeleteWithSkip()
        {
            var db = CreateDb();

            Assert.Throws<InvalidOperationException>(() =>
            {
                var r = db.Table<TestTable>().Where(p => p.Test == "Hello World").Skip(2).Delete();
            });
        }
    }
}

