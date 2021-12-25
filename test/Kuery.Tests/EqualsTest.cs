using System;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests
{
    public class EqualsTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public EqualsTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public abstract class TestObjBase<T>
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }

            public T Data { get; set; }

            public DateTime Date { get; set; }
        }

        public class TestObjString : TestObjBase<string> { }

        void CreateTestTable(DbConnection connection)
        {
            connection.DropTable(nameof(TestObjString));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    if object_id (N'{nameof(TestObjString)}') is null
                        create table [{nameof(TestObjString)}] (
                            {nameof(TestObjString.Id)} integer identity(1,1) primary key not null,
                            {nameof(TestObjString.Data)} nvarchar(50) null,
                            {nameof(TestObjString.Date)} datetime null
                        );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void CanCompareAnyField()
        {
            var n = 20;
            var cq = from i in Enumerable.Range(1, n)
                     select new TestObjString
                     {
                         Data = Convert.ToString(i),
                         Date = new DateTime(2013, 1, i)
                     };
            using var db = fixture.OpenNewConnection();
            CreateTestTable(db);
            db.InsertAll(cq);

            var results = db.Table<TestObjString>().Where(o => o.Data.Equals("10"));
            Assert.Equal(1, results.Count());
            Assert.Equal("10", results.FirstOrDefault().Data);

            results = db.Table<TestObjString>().Where(o => o.Id.Equals(10));
            Assert.Equal(1, results.Count());
            Assert.Equal("10", results.FirstOrDefault().Data);

            var date = new DateTime(2013, 1, 10);
            results = db.Table<TestObjString>().Where(o => o.Date.Equals(date));
            Assert.Equal(1, results.Count());
            Assert.Equal("10", results.FirstOrDefault().Data);
        }
    }
}
