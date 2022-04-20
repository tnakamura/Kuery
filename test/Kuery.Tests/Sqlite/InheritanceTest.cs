using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class InheritanceTest : IClassFixture<SqliteFixture>
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

        readonly SqliteFixture _fixture;

        public InheritanceTest(SqliteFixture fixture)
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
