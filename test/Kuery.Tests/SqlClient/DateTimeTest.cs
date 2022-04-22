using System;
using System.Data.Common;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class DateTimeTest : IClassFixture<SqlClientFixture>
    {
        public SqlClientFixture fixture;

        public DateTimeTest(SqlClientFixture fixture)
        {
            this.fixture = fixture;
        }

        class DateTimeTestObj
        {
            [PrimaryKey, AutoIncrement]
            public int Id { get; set; }

            public DateTime ModifiedTime { get; set; }
        }

        static void CreateTestTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(DateTimeTestObj)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(DateTimeTestObj)} (
                        {nameof(DateTimeTestObj.Id)} integer primary key identity,
                        {nameof(DateTimeTestObj.ModifiedTime)} datetime not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public async Task TestAsyncDateTime()
        {
            using var db = fixture.OpenNewConnection();
            CreateTestTable(db);

            //
            // Ticks
            //
            var o = new DateTimeTestObj
            {
                ModifiedTime = new DateTime(2012, 1, 14, 3, 2, 1),
            };
            await db.InsertAsync(o);

            var o2 = await db.GetAsync<DateTimeTestObj>(o.Id);
            Assert.Equal(o.ModifiedTime, o2.ModifiedTime);
        }

        [Fact]
        public void TestDateTime()
        {
            using var db = fixture.OpenNewConnection();
            CreateTestTable(db);

            DateTimeTestObj o, o2;

            //
            // Ticks
            //
            o = new DateTimeTestObj
            {
                ModifiedTime = new DateTime(2012, 1, 14, 3, 2, 1),
            };
            db.Insert(o);
            o2 = db.Get<DateTimeTestObj>(o.Id);
            Assert.Equal(o.ModifiedTime, o2.ModifiedTime);
        }

        class NullableDateObj
        {
            public DateTime? Time { get; set; }
        }

        static void CreateNullableTestTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(NullableDateObj)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(NullableDateObj)} (
                        {nameof(NullableDateObj.Time)} datetime null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public async Task LinqNullable()
        {
            using var db = fixture.OpenNewConnection();
            CreateNullableTestTable(db);

            var epochTime = new DateTime(1970, 1, 1);

            await db.InsertAsync(new NullableDateObj { Time = epochTime });
            await db.InsertAsync(new NullableDateObj { Time = new DateTime(1980, 7, 23) });
            await db.InsertAsync(new NullableDateObj { Time = null });
            await db.InsertAsync(new NullableDateObj { Time = new DateTime(2019, 1, 23) });

            var res = await db.Table<NullableDateObj>()
                .Where(x => x.Time == epochTime)
                .ToListAsync();
            Assert.Single(res);

            res = await db.Table<NullableDateObj>()
                .Where(x => x.Time > epochTime)
                .ToListAsync();
            Assert.Equal(2, res.Count);
        }
    }
}

