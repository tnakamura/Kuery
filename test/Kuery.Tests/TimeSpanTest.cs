using System;
using System.Data.Common;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests
{
    public class TimeSpanTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public TimeSpanTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        class TimeSpanTestObj
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }

            public string Name { get; set; }

            public TimeSpan Duration { get; set; }
        }

        static void CreateTestTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(TimeSpanTestObj)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(TimeSpanTestObj)} (
                        {nameof(TimeSpanTestObj.Id)} integer primary key autoincrement,
                        {nameof(TimeSpanTestObj.Name)} text null,
                        {nameof(TimeSpanTestObj.Duration)} time not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public async Task TestAsyncTimeSpan()
        {
            using var db = fixture.OpenNewConnection();
            CreateTestTable(db);

            var o = new TimeSpanTestObj
            {
                Duration = new TimeSpan(12, 33, 20),
            };
            await db.InsertAsync(o);

            var o2 = await db.GetAsync<TimeSpanTestObj>(o.Id);
            Assert.Equal(o.Duration, o2.Duration);
        }

        [Fact]
        public void TestTimeSpan()
        {
            using var db = fixture.OpenNewConnection();
            CreateTestTable(db);

            var o = new TimeSpanTestObj
            {
                Duration = new TimeSpan(12, 33, 20),
            };
            db.Insert(o);

            var o2 = db.Get<TimeSpanTestObj>(o.Id);
            Assert.Equal(o.Duration, o2.Duration);
        }
    }
}
