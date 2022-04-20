using System;
using System.Diagnostics;
using System.Linq;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class InsertTest : IClassFixture<SqlClientFixture>, IDisposable
    {
        readonly SqlClientFixture fixture;

        readonly SqlConnection connection;

        public InsertTest(SqlClientFixture fixture)
        {
            this.fixture = fixture;
            connection = fixture.OpenNewConnection();

            DropTestTables(connection);
            CreateTestTable(connection);
        }

        public void Dispose()
        {
            DropTestTables(connection);
            connection?.Close();
        }

        public class InsertTestObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }

            public string Text { get; set; }

            public override string ToString()
            {
                return string.Format("[InsertTestObj: Id={0}, Text={1}]", Id, Text);
            }
        }

        public class InsertTestObj2
        {
            [PrimaryKey]
            public int Id { get; set; }

            public string Text { get; set; }

            public override string ToString()
            {
                return string.Format("[InsertTestObj: Id={0}, Text={1}]", Id, Text);
            }

        }

        public class OneColumnObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }
        }

        public class UniqueObj
        {
            [PrimaryKey]
            public int Id { get; set; }
        }

        private void DropTestTables(SqlConnection connection)
        {
            connection.DropTable(nameof(InsertTestObj));
            connection.DropTable(nameof(InsertTestObj2));
            connection.DropTable(nameof(OneColumnObj));
            connection.DropTable(nameof(UniqueObj));
        }

        private void CreateTestTable(SqlConnection connection)
        {
            DropTestTables(connection);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(InsertTestObj)}] (
                        {nameof(InsertTestObj.Id)} integer primary key identity,
                        {nameof(InsertTestObj.Text)} text null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(InsertTestObj2)}] (
                        {nameof(InsertTestObj2.Id)} integer primary key,
                        {nameof(InsertTestObj2.Text)} text null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(OneColumnObj)}] (
                        {nameof(OneColumnObj.Id)} integer primary key identity
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(UniqueObj)}] (
                        {nameof(UniqueObj.Id)} integer primary key
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void InsertALot()
        {
            int n = 100/*00*/;
            var q = from i in Enumerable.Range(1, n)
                    select new InsertTestObj()
                    {
                        Text = "I am"
                    };
            var objs = q.ToArray();

            var sw = new Stopwatch();
            sw.Start();

            var numIn = connection.InsertAll(objs);

            sw.Stop();

            Assert.Equal(numIn, n);

            var inObjs = connection.Query<InsertTestObj>(
                $"select * from {nameof(InsertTestObj)}")
                .ToArray();
            for (var i = 0; i < inObjs.Length; i++)
            {
                Assert.Equal(i + 1, objs[i].Id);
                Assert.Equal(i + 1, inObjs[i].Id);
                Assert.Equal("I am", inObjs[i].Text);
            }

            var numCount = connection.ExecuteScalar<int>(
                $"select count(*) from {nameof(InsertTestObj)}");
            Assert.Equal(numCount, n);
        }

        [Fact]
        public void InsertTwoTimes()
        {
            var obj1 = new InsertTestObj() { Text = "GLaDOS loves testing!" };
            var obj2 = new InsertTestObj() { Text = "Keep testing, just keep testing" };


            var numIn1 = connection.Insert(obj1);
            var numIn2 = connection.Insert(obj2);
            Assert.Equal(1, numIn1);
            Assert.Equal(1, numIn2);

            var result = connection.Query<InsertTestObj>(
                $"select * from {nameof(InsertTestObj)}").ToList();
            Assert.Equal(2, result.Count);
            Assert.Equal(obj1.Text, result[0].Text);
            Assert.Equal(obj2.Text, result[1].Text);
        }

        [Fact]
        public void InsertIntoTwoTables()
        {
            var obj1 = new InsertTestObj() { Text = "GLaDOS loves testing!" };
            var obj2 = new InsertTestObj2() { Text = "Keep testing, just keep testing" };

            var numIn1 = connection.Insert(obj1);
            Assert.Equal(1, numIn1);
            var numIn2 = connection.Insert(obj2);
            Assert.Equal(1, numIn2);

            var result1 = connection.Query<InsertTestObj>(
                $"select * from {nameof(InsertTestObj)}").ToList();
            Assert.Equal(numIn1, result1.Count);
            Assert.Equal(obj1.Text, result1.First().Text);

            var result2 = connection.Query<InsertTestObj2>(
                $"select * from {nameof(InsertTestObj2)}").ToList();
            Assert.Equal(numIn2, result2.Count);
        }

        [Fact]
        public void InsertIntoOneColumnAutoIncrementTable()
        {
            var obj = new OneColumnObj();
            connection.Insert(obj);

            var result = connection.Get<OneColumnObj>(1);
            Assert.Equal(1, result.Id);
        }

        [Fact]
        public void InsertAllSuccessOutsideTransaction()
        {
            var testObjects = Enumerable.Range(1, 20).Select(i => new UniqueObj { Id = i }).ToList();

            connection.InsertAll(testObjects);

            Assert.Equal(testObjects.Count, connection.Table<UniqueObj>().Count());
        }

        [Fact]
        public void InsertAllFailureOutsideTransaction()
        {
            var testObjects = Enumerable.Range(1, 20)
                .Select(i => new UniqueObj { Id = i })
                .ToList();
            testObjects[testObjects.Count - 1].Id = 1; // causes the insert to fail because of duplicate key

            Assert.Throws<SqlException>(() =>
            {
                connection.InsertAll(testObjects);
            });

            Assert.Equal(
                testObjects.Count - 1,
                connection.Table<UniqueObj>().Count());
        }

        [Fact]
        public void InsertAllSuccessInsideTransaction()
        {
            var testObjects = Enumerable.Range(1, 20).Select(i => new UniqueObj { Id = i }).ToList();

            var tx = connection.BeginTransaction();
            connection.InsertAll(testObjects, tx);
            tx.Commit();

            Assert.Equal(testObjects.Count, connection.Table<UniqueObj>().Count());
        }

        [Fact]
        public void InsertAllFailureInsideTransaction()
        {
            var testObjects = Enumerable.Range(1, 20)
                .Select(i => new UniqueObj { Id = i })
                .ToList();
            testObjects[testObjects.Count - 1].Id = 1; // causes the insert to fail because of duplicate key

            Assert.Throws<SqlException>(() =>
            {
                var tx = connection.BeginTransaction();
                try
                {
                    connection.InsertAll(testObjects, tx);
                    tx.Commit();
                }
                catch
                {
                    tx.Rollback();
                    throw;
                }
            });

            Assert.Equal(0, connection.Table<UniqueObj>().Count());
        }

        [Fact]
        public void InsertOrReplace()
        {
            connection.InsertAll(
                from i in Enumerable.Range(1, 20)
                select new InsertTestObj
                {
                    Text = "#" + i,
                });

            Assert.Equal(20, connection.Table<InsertTestObj>().Count());

            var t = new InsertTestObj { Id = 5, Text = "Foo", };
            connection.InsertOrReplace(t);

            var r = (from x in connection.Table<InsertTestObj>() orderby x.Id select x).ToList();
            Assert.Equal(20, r.Count);
            Assert.Equal("Foo", r[4].Text);
        }
    }
}
