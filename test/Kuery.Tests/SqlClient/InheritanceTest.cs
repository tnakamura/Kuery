using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class InheritanceTest : IClassFixture<SqlClientFixture>
    {
        class Base
        {
            [PrimaryKey]
            public int Id { get; set; }

            public string BaseProp { get; set; }
        }

        class Derived : Base
        {
            public string DerivedProp { get; set; }
        }

        readonly SqlClientFixture _fixture;

        public InheritanceTest(SqlClientFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public void InheritanceWorks()
        {
            using var connection = _fixture.CreateConnection();

            var mapping = connection.GetMapping<Derived>();

            Assert.Equal(3, mapping.Columns.Count);
            Assert.Equal("Id", mapping.PK.Name);
        }
    }
}
