using System.Linq;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Kuery.Tests.Npgsql
{
    public class EnumTest : IClassFixture<NpgsqlFixture>
    {
        readonly NpgsqlFixture fixture;

        public EnumTest(NpgsqlFixture fixture)
        {
            this.fixture = fixture;
        }

        public enum TestEnum
        {
            Value1,

            Value2,

            Value3
        }

        [StoreAsText]
        public enum StringTestEnum
        {
            Value1,

            Value2,

            Value3
        }

        public class EnumTestObj
        {
            [PrimaryKey]
            public int Id { get; set; }
            public TestEnum Value { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}, Value={1}]", Id, Value);
            }

        }

        public class StringTestObj
        {
            [PrimaryKey]
            public int Id { get; set; }
            public StringTestEnum Value { get; set; }

            public override string ToString()
            {
                return string.Format("[StringTestObj: Id={0}, Value={1}]", Id, Value);
            }

        }

        void CreateTestTable(global::Npgsql.NpgsqlConnection connection)
        {
            connection.DropTable(nameof(EnumTestObj));
            connection.DropTable(nameof(StringTestObj));
            connection.DropTable(nameof(ByteTestObj));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table ""{nameof(EnumTestObj)}"" (
                        ""{nameof(EnumTestObj.Id)}"" integer primary key,
                        ""{nameof(EnumTestObj.Value)}"" integer not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table ""{nameof(StringTestObj)}"" (
                        ""{nameof(StringTestObj.Id)}"" integer primary key,
                        ""{nameof(StringTestObj.Value)}"" varchar(50) not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table ""{nameof(ByteTestObj)}"" (
                        ""{nameof(ByteTestObj.Id)}"" integer primary key,
                        ""{nameof(ByteTestObj.Value)}"" smallint not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void ShouldPersistAndReadEnum()
        {
            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            var obj1 = new EnumTestObj() { Id = 1, Value = TestEnum.Value2 };
            var obj2 = new EnumTestObj() { Id = 2, Value = TestEnum.Value3 };

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.Equal(1, numIn1);
            Assert.Equal(1, numIn2);

            var result = con.Query<EnumTestObj>(
                $"select * from \"{nameof(EnumTestObj)}\"")
                .ToList();
            Assert.Equal(2, result.Count);
            Assert.Equal(obj1.Value, result[0].Value);
            Assert.Equal(obj2.Value, result[1].Value);

            Assert.Equal(obj1.Id, result[0].Id);
            Assert.Equal(obj2.Id, result[1].Id);
        }

        [Fact]
        public void ShouldPersistAndReadStringEnum()
        {
            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            var obj1 = new StringTestObj() { Id = 1, Value = StringTestEnum.Value2 };
            var obj2 = new StringTestObj() { Id = 2, Value = StringTestEnum.Value3 };

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.Equal(1, numIn1);
            Assert.Equal(1, numIn2);

            var result = con.Query<StringTestObj>(
                $"select * from \"{nameof(StringTestObj)}\"")
                .ToList();
            Assert.Equal(2, result.Count);
            Assert.Equal(obj1.Value, result[0].Value);
            Assert.Equal(obj2.Value, result[1].Value);

            Assert.Equal(obj1.Id, result[0].Id);
            Assert.Equal(obj2.Id, result[1].Id);
        }

        public enum ByteTestEnum : byte
        {
            Value1 = 1,

            Value2 = 2,

            Value3 = 3
        }

        public class ByteTestObj
        {
            [PrimaryKey]
            public int Id { get; set; }

            public ByteTestEnum Value { get; set; }

            public override string ToString()
            {
                return string.Format("[ByteTestObj: Id={0}, Value={1}]", Id, Value);
            }
        }

        [Fact]
        public void ShouldPersistAndReadByteEnum()
        {
            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            var obj1 = new ByteTestObj() { Id = 1, Value = ByteTestEnum.Value2 };
            var obj2 = new ByteTestObj() { Id = 2, Value = ByteTestEnum.Value3 };

            var numIn1 = con.Insert(obj1);
            var numIn2 = con.Insert(obj2);
            Assert.Equal(1, numIn1);
            Assert.Equal(1, numIn2);

            var result = con.Query<ByteTestObj>(
                $"select * from \"{nameof(ByteTestObj)}\" order by \"Id\"")
                .ToList();
            Assert.Equal(2, result.Count);
            Assert.Equal(obj1.Value, result[0].Value);
            Assert.Equal(obj2.Value, result[1].Value);

            Assert.Equal(obj1.Id, result[0].Id);
            Assert.Equal(obj2.Id, result[1].Id);
        }
    }
}
