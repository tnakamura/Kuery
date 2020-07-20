using System;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Diagnostics;

using Xunit;


namespace Kuery.Tests
{
    public class BooleanTest : IClassFixture<SqlServerFixture>
    {
        public class VO
        {
            [AutoIncrement, PrimaryKey]
            public int ID { get; set; }

            public bool Flag { get; set; }

            public string Text { get; set; }
        }

        readonly SqlServerFixture fixture;

        public BooleanTest(SqlServerFixture fixture)
        {
            this.fixture = fixture;
        }

        void CreateTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
                    if object_id (N'VO') is not null
                        drop table VO;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
                    if object_id (N'VO') is null
                        create table VO (
                            Id integer identity(1,1) primary key not null,
                            Flag bit not null,
                            Text nvarchar(64) null
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
                    Flag = (i % 3 == 0),
                    Text = $"VO{i}",
                });
            }

            Assert.Equal(
                4,
                db.ExecuteScalar<int>(
                    "SELECT COUNT(*) FROM VO Where Flag = @flag",
                    new { flag = true }));
            Assert.Equal(
                6,
                db.ExecuteScalar<int>(
                    "SELECT COUNT(*) FROM VO Where Flag = @flag",
                    new { flag = false }));
        }
    }
}
