using System;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class ContainsTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public ContainsTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public class ContainsTestObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }

            public string Name { get; set; }
        }

        static void CreateTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(ContainsTestObj)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(ContainsTestObj)} (
                        {nameof(ContainsTestObj.Id)} integer primary key autoincrement,
                        {nameof(ContainsTestObj.Name)} nvarchar(256) not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void ContainsConstantData()
        {
            int n = 20;
            var cq = from i in Enumerable.Range(1, n)
                     select new ContainsTestObj()
                     {
                         Name = i.ToString()
                     };

            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            con.InsertAll(cq);

            var tensq = new string[] { "0", "10", "20" };
            var tens = (
                from o in con.Table<ContainsTestObj>()
                where tensq.Contains(o.Name)
                select o
            ).ToList();
            Assert.Equal(2, tens.Count);

            var moreq = new string[] { "0", "x", "99", "10", "20", "234324" };
            var more = (
                from o in con.Table<ContainsTestObj>()
                where moreq.Contains(o.Name)
                select o
            ).ToList();
            Assert.Equal(2, more.Count);
        }

        [Fact]
        public void ContainsQueriedData()
        {
            int n = 20;
            var cq = from i in Enumerable.Range(1, n)
                     select new ContainsTestObj()
                     {
                         Name = i.ToString()
                     };

            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            con.InsertAll(cq);

            var tensq = new string[] { "0", "10", "20" };
            var tens = (
                from o in con.Table<ContainsTestObj>()
                where tensq.Contains(o.Name)
                select o
            ).ToList();
            Assert.Equal(2, tens.Count);

            var moreq = new string[] { "0", "x", "99", "10", "20", "234324" };
            var more = (
                from o in con.Table<ContainsTestObj>()
                where moreq.Contains(o.Name)
                select o
            ).ToList();
            Assert.Equal(2, more.Count);

            var moreq2 = moreq.ToList();
            var more2 = (
                from o in con.Table<ContainsTestObj>()
                where moreq2.Contains(o.Name)
                select o
            ).ToList();
            Assert.Equal(2, more2.Count);
        }
    }
}
