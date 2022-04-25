using System.Collections.Generic;
using System.Data.Common;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class MappingTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public MappingTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        [Table("AGoodTableName")]
        class AFunnyTableName
        {
            [PrimaryKey]
            public int Id { get; set; }

            [Column("AGoodColumnName")]
            public string AFunnyColumnName { get; set; }
        }

        [Fact]
        public void HasGoodNames()
        {
            var mapping = SqlHelper.GetMapping<AFunnyTableName>();
            Assert.Equal("AGoodTableName", mapping.TableName);
            Assert.Equal("Id", mapping.Columns[0].Name);
            Assert.Equal("AGoodColumnName", mapping.Columns[1].Name);
        }

        class OverrideNamesBase
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }

            public virtual string Name { get; set; }

            public virtual string Value { get; set; }
        }

        class OverrideNamesClass : OverrideNamesBase
        {
            [Column("n")]
            public override string Name { get; set; }

            [Column("v")]
            public override string Value { get; set; }
        }

        static void CreateOverrideNamesClass(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists OverrideNamesClass;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists OverrideNamesClass (
                        Id integer primary key autoincrement,
                        n nvarchar(50) null,
                        v nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void OverrideNames()
        {
            using var con = fixture.OpenNewConnection();
            CreateOverrideNamesClass(con);

            var o = new OverrideNamesClass
            {
                Name = "Foo",
                Value = "Bar",
            };
            con.Insert(o);

            var oo = con.Table<OverrideNamesClass>().First();
            Assert.Equal("Foo", oo.Name);
            Assert.Equal("Bar", oo.Value);
        }

        [Table("foo")]
        public class Foo
        {
            [Column("baz")]
            public int Bar { get; set; }
        }

        static void CreateFooTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists foo;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists foo (
                        baz integer primary key not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void WhereAndOrder()
        {
            using var con = fixture.OpenNewConnection();
            CreateFooTable(con);

            con.Insert(new Foo { Bar = 42 });
            con.Insert(new Foo { Bar = 69 });

            var found42 = con.Table<Foo>().Where(f => f.Bar == 42).FirstOrDefault();
            Assert.NotNull(found42);

            var ordered = new List<Foo>(con.Table<Foo>().OrderByDescending(f => f.Bar));
            Assert.Equal(2, ordered.Count);
            Assert.Equal(69, ordered[0].Bar);
            Assert.Equal(42, ordered[1].Bar);
        }


        public class OnlyKeyModel
        {
            [PrimaryKey]
            public string MyModelId { get; set; }
        }

        static void CreateOnlyKeyModelTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists OnlyKeyModel;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists OnlyKeyModel (
                        MyModelId nvarchar(250) primary key
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void OnlyKey()
        {
            using var con = fixture.OpenNewConnection();
            CreateOnlyKeyModelTable(con);

            con.InsertOrReplace(new OnlyKeyModel { MyModelId = "Foo" });
            var foo = con.Get<OnlyKeyModel>("Foo");
            Assert.Equal("Foo", foo.MyModelId);

            con.Insert(new OnlyKeyModel { MyModelId = "Bar" });
            var bar = con.Get<OnlyKeyModel>("Bar");
            Assert.Equal("Bar", bar.MyModelId);

            con.Update(new OnlyKeyModel { MyModelId = "Foo" });
            var foo2 = con.Get<OnlyKeyModel>("Foo");
            Assert.Equal("Foo", foo2.MyModelId);
        }
    }
}

