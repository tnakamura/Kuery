using System;
using System.Linq;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class EqualsTest : IClassFixture<SqlClientFixture>
    {
        readonly SqlClientFixture fixture;

        public EqualsTest(SqlClientFixture fixture)
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

        void CreateTestTable(SqlConnection connection)
        {
            connection.DropTable(nameof(TestObjString));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(TestObjString)}] (
                        {nameof(TestObjString.Id)} integer primary key identity,
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
