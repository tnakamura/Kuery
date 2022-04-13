using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class SkipTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public SkipTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public class TestObj
        {
            [PrimaryKey]
            public string Id { get; set; }

            public int Order { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}, Order={1}]", Id, Order);
            }

        }

        [Fact]
        public void Skip()
        {
            using var connection = fixture.CreateConnection();
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
                    create table if not exists [TestObj] (
                        [Id] nvarchar(50) not null primary key,
                        [Order] integer not null
                    );";
                command.ExecuteNonQuery();
            }

            var n = 100;
            var cq = from i in Enumerable.Range(1, n)
                     select new TestObj()
                     {
                         Id = Guid.NewGuid().ToString(),
                         Order = i
                     };
            var objs = cq.ToArray();

            var numIn = connection.InsertAll(objs);
            Assert.Equal(numIn, n);

            var q = from o in connection.Table<TestObj>()
                    orderby o.Order
                    select o;

            var qs1 = q.Skip(1);
            var s1 = qs1.ToList();
            Assert.Equal(n - 1, s1.Count);
            Assert.Equal(2, s1[0].Order);

            var qs5 = q.Skip(5);
            var s5 = qs5.ToList();
            Assert.Equal(n - 5, s5.Count);
            Assert.Equal(6, s5[0].Order);
        }
    }
}
