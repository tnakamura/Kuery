using System;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class GuidTest : IClassFixture<SqlClientFixture>, IDisposable
    {
        readonly SqlClientFixture fixture;

        public GuidTest(SqlClientFixture fixture)
        {
            this.fixture = fixture;
        }

        public void Dispose()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                DropTable(connection);
            }
        }

        public class GuidTestObj
        {
            [PrimaryKey]
            [AutoIncrement]
            public Guid Id { get; set; }

            public string Text { get; set; }
        }

        private static void DropTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(GuidTestObj)};";
                cmd.ExecuteNonQuery();
            }
        }

        private static void CreateTable(DbConnection connection)
        {
            DropTable(connection);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(GuidTestObj)} (
                        {nameof(GuidTestObj.Id)} uniqueidentifier primary key,
                        {nameof(GuidTestObj.Text)} nvarchar(64) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void ShouldPersistAndReadGuid()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            var obj1 = new GuidTestObj
            {
                Id = new Guid("36473164-C9E4-4CDF-B266-A0B287C85623"),
                Text = "First Guid Object",
            };
            var obj2 = new GuidTestObj
            {
                Id = new Guid("BC5C4C4A-CA57-4B61-8B53-9FD4673528B6"),
                Text = "Second Guid Object",
            };

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.Equal(1, numIn1);
            Assert.Equal(1, numIn2);

            var result = con.Query<GuidTestObj>(
                $@"select *
                   from {nameof(GuidTestObj)}
                   order by {nameof(GuidTestObj.Text)}")
                .ToList();
            Assert.Equal(2, result.Count);
            Assert.Equal(obj1.Text, result[0].Text);
            Assert.Equal(obj2.Text, result[1].Text);

            Assert.Equal(obj1.Id, result[0].Id);
            Assert.Equal(obj2.Id, result[1].Id);

            con.Close();
        }

        [Fact]
        public void AutoGuid_HasGuid()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            var guid1 = new Guid("36473164-C9E4-4CDF-B266-A0B287C85623");
            var guid2 = new Guid("BC5C4C4A-CA57-4B61-8B53-9FD4673528B6");

            var obj1 = new GuidTestObj() { Id = guid1, Text = "First Guid Object" };
            var obj2 = new GuidTestObj() { Id = guid2, Text = "Second Guid Object" };

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.Equal(guid1, obj1.Id);
            Assert.Equal(guid2, obj2.Id);

            con.Close();
        }

        [Fact]
        public void AutoGuid_EmptyGuid()
        {
            var con = fixture.OpenNewConnection();
            CreateTable(con);

            var obj1 = new GuidTestObj
            {
                Text = "First Guid Object",
            };
            var obj2 = new GuidTestObj
            {
                Text = "Second Guid Object",
            };
            Assert.Equal(Guid.Empty, obj1.Id);
            Assert.Equal(Guid.Empty, obj2.Id);

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.NotEqual(Guid.Empty, obj1.Id);
            Assert.NotEqual(Guid.Empty, obj2.Id);
            Assert.NotEqual(obj1.Id, obj2.Id);

            con.Close();
        }
    }
}
