using System.Linq;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Kuery.Tests.Npgsql
{
    public class EnumNullableTest : IClassFixture<NpgsqlFixture>
    {
        readonly NpgsqlFixture fixture;

        public EnumNullableTest(NpgsqlFixture fixture)
        {
            this.fixture = fixture;
        }

        public enum TestEnum
        {
            Value1,

            Value2,

            Value3
        }

        public class EnumNullableTestObj
        {
            [PrimaryKey]
            public int Id { get; set; }

            public TestEnum? Value { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}, Value={1}]", Id, Value);
            }

        }

        void CreateTable(global::Npgsql.NpgsqlConnection connection)
        {
            connection.DropTable(nameof(EnumNullableTestObj));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table ""{nameof(EnumNullableTestObj)}"" (
                        ""{nameof(EnumNullableTestObj.Id)}"" integer primary key,
                        ""{nameof(EnumNullableTestObj.Value)}"" integer null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void ShouldPersistAndReadEnum()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            var obj1 = new EnumNullableTestObj() { Id = 1, Value = TestEnum.Value2 };
            var obj2 = new EnumNullableTestObj() { Id = 2, Value = TestEnum.Value3 };

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.Equal(1, numIn1);
            Assert.Equal(1, numIn2);

            var result = con.Query<EnumNullableTestObj>(
                $"select * from \"{nameof(EnumNullableTestObj)}\"").ToList();
            Assert.Equal(2, result.Count);
            Assert.Equal(obj1.Value, result[0].Value);
            Assert.Equal(obj2.Value, result[1].Value);

            Assert.Equal(obj1.Id, result[0].Id);
            Assert.Equal(obj2.Id, result[1].Id);
        }
    }
}