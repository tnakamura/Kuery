using System;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Diagnostics;

using Xunit;


namespace Kuery.Tests.Sqlite
{
    public class BooleanTest : IClassFixture<SqliteFixture>
    {
        public class VO
        {
            [AutoIncrement, PrimaryKey]
            public int ID { get; set; }

            public bool Flag { get; set; }

            public string Text { get; set; }
        }

        readonly SqliteFixture fixture;

        public BooleanTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        void CreateTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
                    drop table if exists VO;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
                    create table if not exists VO (
                        Id integer primary key autoincrement,
                        Flag bit not null,
                        Text text null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void TestBoolean()
        {
            using var db = fixture.OpenNewConnection();
            CreateTable(db);

            for (var i = 0; i < 10; i++)
            {
                db.Insert(new VO()
                {
                    Flag = i % 3 == 0,
                    Text = $"VO{i}",
                });
            }

            Assert.Equal(
                4,
                db.ExecuteScalar<int>(
                    "SELECT COUNT(*) FROM VO Where Flag = $flag",
                    new { flag = true }));
            Assert.Equal(
                6,
                db.ExecuteScalar<int>(
                    "SELECT COUNT(*) FROM VO Where Flag = $flag",
                    new { flag = false }));
        }
    }
}
