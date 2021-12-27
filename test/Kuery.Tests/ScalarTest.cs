using System;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests
{
    public class ScalarTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public ScalarTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        class ScalarTestTable
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }

            public int Two { get; set; }
        }

        static void CreateTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(ScalarTestTable)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(ScalarTestTable)} (
                        {nameof(ScalarTestTable.Id)} integer primary key autoincrement,
                        {nameof(ScalarTestTable.Two)} integer not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        const int Count = 100;

        DbConnection CreateDb()
        {
            var con = fixture.OpenNewConnection();
            CreateTable(con);

            var items = from i in Enumerable.Range(0, Count)
                        select new ScalarTestTable
                        {
                            Two = 2,
                        };
            con.InsertAll(items);
            return con;
        }

        [Fact]
        public void Int32()
        {
            var db = CreateDb();

            var r = db.ExecuteScalar<int>(
                $"SELECT SUM(Two) FROM {nameof(ScalarTestTable)}");

            Assert.Equal(Count * 2, r);
        }

        [Fact]
        public void SelectSingleRowValue()
        {
            var db = CreateDb();

            var r = db.ExecuteScalar<int>(
                $"SELECT Two FROM {nameof(ScalarTestTable)} WHERE Id = 1 LIMIT 1");

            Assert.Equal(2, r);
        }

        [Fact]
        public void SelectNullableSingleRowValue()
        {
            var db = CreateDb();

            var r = db.ExecuteScalar<int?>(
                $"SELECT Two FROM {nameof(ScalarTestTable)} WHERE Id = 1 LIMIT 1");

            Assert.True(r.HasValue);
            Assert.Equal(2, r);
        }

        [Fact]
        public void SelectNoRowValue()
        {
            var db = CreateDb();

            var r = db.ExecuteScalar<int?>(
                $"SELECT Two FROM {nameof(ScalarTestTable)} WHERE Id = 999");

            Assert.False(r.HasValue);
        }

        [Fact]
        public void SelectNullRowValue()
        {
            var db = CreateDb();

            var r = db.ExecuteScalar<int?>(
                $"SELECT null AS Unknown FROM {nameof(ScalarTestTable)} WHERE Id = 1 LIMIT 1");

            Assert.False(r.HasValue);
        }
    }
}

