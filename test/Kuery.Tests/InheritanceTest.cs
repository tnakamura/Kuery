using Xunit;

namespace Kuery.Tests
{
    public class InheritanceTest 
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

        [Fact]
        public void InheritanceWorks()
        {
            var mapping = SqlMapper.GetMapping<Derived>();

            Assert.Equal(3, mapping.Columns.Count);
            Assert.Equal("Id", mapping.PK.Name);
        }
    }
}
