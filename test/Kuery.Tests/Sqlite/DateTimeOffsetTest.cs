using System;
using System.Data.Common;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class DateTimeOffsetTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public DateTimeOffsetTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        class DtoTestObj
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }

            public string Name { get; set; }

            public DateTimeOffset ModifiedTime { get; set; }
        }

        static void CreateTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(DtoTestObj)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(DtoTestObj)} (
                        {nameof(DtoTestObj.Id)} integer primary key autoincrement,
                        {nameof(DtoTestObj.Name)} nvarchar(50) null,
                        {nameof(DtoTestObj.ModifiedTime)} datetimeoffset null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public async Task TestAsyncDateTimeOffset()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            var o = new DtoTestObj
            {
                ModifiedTime = new DateTimeOffset(2012, 1, 14, 3, 2, 1, TimeSpan.Zero),
            };
            await con.InsertAsync(o);

            var o2 = await con.GetAsync<DtoTestObj>(o.Id);
            Assert.Equal(o.ModifiedTime, o2.ModifiedTime);
        }

        [Fact]
        public void TestDateTimeOffset()
        {
            using var con = fixture.OpenNewConnection();
            CreateTable(con);

            var o = new DtoTestObj
            {
                ModifiedTime = new DateTimeOffset(2012, 1, 14, 3, 2, 1, TimeSpan.Zero),
            };
            con.Insert(o);

            var o2 = con.Get<DtoTestObj>(o.Id);
            Assert.Equal(o.ModifiedTime, o2.ModifiedTime);
        }
    }
}

