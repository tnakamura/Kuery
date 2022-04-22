using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class IgnoreTest : IClassFixture<SqlClientFixture>
    {
        readonly SqlClientFixture fixture;

        public IgnoreTest(SqlClientFixture fixture)
        {
            this.fixture = fixture;
        }

        public class IgnoreTestObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }

            public string Text { get; set; }

            [Ignore]
            public Dictionary<int, string> Edibles
            {
                get { return _edibles; }
                set { _edibles = value; }
            }

            protected Dictionary<int, string> _edibles = new Dictionary<int, string>();

            [Ignore]
            public string IgnoredText { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}]", Id);
            }
        }

        void CreateIgnoreTestObjTable(SqlConnection connection)
        {
            connection.DropTable(nameof(IgnoreTestObj));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(IgnoreTestObj)} (
                        {nameof(IgnoreTestObj.Id)} integer primary key identity,
                        {nameof(IgnoreTestObj.Text)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void MappingIgnoreColumn()
        {
            using var con = fixture.OpenNewConnection();

            var m = con.GetMapping<IgnoreTestObj>();

            Assert.Equal(2, m.Columns.Count);
        }

        [Fact]
        public void InsertSucceeds()
        {
            using var con = fixture.OpenNewConnection();
            CreateIgnoreTestObjTable(con);

            var o = new IgnoreTestObj
            {
                Text = "Hello",
                IgnoredText = "World",
            };

            con.Insert(o);

            Assert.Equal(1, o.Id);
        }

        [Fact]
        public void GetDoesntHaveIgnores()
        {
            using var con = fixture.OpenNewConnection();
            CreateIgnoreTestObjTable(con);

            var o = new IgnoreTestObj
            {
                Text = "Hello",
                IgnoredText = "World",
            };

            con.Insert(o);

            var oo = con.Table<IgnoreTestObj>()
                .Where(x => x.Text == "Hello")
                .First();

            Assert.Equal("Hello", oo.Text);
            Assert.Null(oo.IgnoredText);
        }

        public class BaseClass
        {
            [Ignore]
            public string ToIgnore
            {
                get;
                set;
            }
        }

        public class IgnoreInheritTableClass : BaseClass
        {
            public string Name { get; set; }
        }

        void CreateIgnoreInheritTableClassTable(SqlConnection connection)
        {
            connection.DropTable(nameof(IgnoreInheritTableClass));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(IgnoreInheritTableClass)}] (
                        {nameof(IgnoreInheritTableClass.Name)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void BaseIgnores()
        {
            using var con = fixture.OpenNewConnection();
            CreateIgnoreInheritTableClassTable(con);

            var o = new IgnoreInheritTableClass
            {
                ToIgnore = "Hello",
                Name = "World",
            };

            con.Insert(o);

            var oo = con.Table<IgnoreInheritTableClass>().First();

            Assert.Null(oo.ToIgnore);
            Assert.Equal("World", oo.Name);
        }

        public class RedefinedBaseClass
        {
            public string Name { get; set; }
            public List<string> Values { get; set; }
        }

        public class RedefinedClass : RedefinedBaseClass
        {
            [Ignore]
            public new List<string> Values { get; set; }
            public string Value { get; set; }
        }

        void CreateRedefinedClassTable(SqlConnection connection)
        {
            connection.DropTable(nameof(RedefinedClass));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(RedefinedClass)}] (
                        {nameof(RedefinedClass.Name)} nvarchar(50) null,
                        {nameof(RedefinedClass.Value)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void RedefinedIgnores()
        {
            using var con = fixture.OpenNewConnection();
            CreateRedefinedClassTable(con);

            var o = new RedefinedClass
            {
                Name = "Foo",
                Value = "Bar",
                Values = new List<string> { "hello", "world" },
            };

            con.Insert(o);

            var oo = con.Table<RedefinedClass>().First();

            Assert.Equal("Foo", oo.Name);
            Assert.Equal("Bar", oo.Value);
            Assert.Null(oo.Values);
        }
    }
}
