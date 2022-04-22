using System.Linq;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class QueryTest : IClassFixture<SqlClientFixture>
    {
        class GenericObject
        {
            public int Value { get; set; }
        }

        readonly SqlClientFixture fixture;

        public QueryTest(SqlClientFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void QueryGenericObject()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                connection.Execute("drop table if exists G;");
                connection.Execute("create table G(Value integer not null);");
                connection.Execute(
                    "insert into G(Value) values (@Value);",
                    param: new { Value = 42 });

                var r = connection.Query<GenericObject>("select * from G");

                Assert.Single(r);
                Assert.Equal(42, r.ElementAt(0).Value);
            }
        }
    }
}
