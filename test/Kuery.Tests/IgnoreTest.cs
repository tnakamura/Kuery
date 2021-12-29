using System.Collections.Generic;
using System.Data.Common;
using Xunit;

namespace Kuery.Tests
{
    public class IgnoreTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public IgnoreTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public class IgnoreTestObj
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }

            public string Text { get; set; }

            [Kuery.Ignore]
            public Dictionary<int, string> Edibles
            {
                get { return this._edibles; }
                set { this._edibles = value; }
            }

            protected Dictionary<int, string> _edibles = new Dictionary<int, string>();

            [Kuery.Ignore]
            public string IgnoredText { get; set; }

            public override string ToString()
            {
                return string.Format("[TestObj: Id={0}]", Id);
            }
        }

        void CreateIgnoreTestObjTable(DbConnection connection)
        {
            connection.DropTable(nameof(IgnoreTestObj));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(IgnoreTestObj)} (
                        {nameof(IgnoreTestObj.Id)} integer primary key autoincrement,
                        {nameof(IgnoreTestObj.Text)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void MappingIgnoreColumn()
        {
            var m = SqlHelper.GetMapping<IgnoreTestObj>();

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

        void CreateIgnoreInheritTableClassTable(DbConnection connection)
        {
            connection.DropTable(nameof(IgnoreInheritTableClass));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists [{nameof(IgnoreInheritTableClass)}] (
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

        void CreateRedefinedClassTable(DbConnection connection)
        {
            connection.DropTable(nameof(RedefinedClass));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists [{nameof(RedefinedClass)}] (
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
