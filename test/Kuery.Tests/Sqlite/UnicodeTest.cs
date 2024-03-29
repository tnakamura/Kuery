using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class UnicodeTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public UnicodeTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public class UnicodeTestObj
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }

            public string Name { get; set; }
        }

        static void CreateTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(UnicodeTestObj)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(UnicodeTestObj)} (
                        {nameof(UnicodeTestObj.Id)} integer primary key autoincrement,
                        {nameof(UnicodeTestObj.Name)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void Insert()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            string testString = "\u2329\u221E\u232A";

            con.Insert(new UnicodeTestObj
            {
                Name = testString,
            });

            var p = con.Get<UnicodeTestObj>(1);

            Assert.Equal(testString, p.Name);
        }

        [Fact]
        public void Query()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            string testString = "\u2329\u221E\u232A";

            con.Insert(new UnicodeTestObj
            {
                Name = testString,
            });

            var ps = (
                from p in con.Table<UnicodeTestObj>()
                where p.Name == testString
                select p
            ).ToList();
            Assert.Single(ps);
            Assert.Equal(testString, ps[0].Name);
        }
    }
}
