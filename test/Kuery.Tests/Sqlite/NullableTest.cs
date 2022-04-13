using System.Data.Common;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class NullableTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public NullableTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public class NullableIntClass
        {
            [PrimaryKey, AutoIncrement]
            public int ID { get; set; }

            public int? NullableInt { get; set; }

            public override bool Equals(object obj)
            {
                NullableIntClass other = (NullableIntClass)obj;
                return ID == other.ID && NullableInt == other.NullableInt;
            }

            public override int GetHashCode()
            {
                return ID.GetHashCode() + NullableInt.GetHashCode();
            }
        }

        static void CreateNullableIntClassTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(NullableIntClass)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(NullableIntClass)} (
                        {nameof(NullableIntClass.ID)} integer primary key autoincrement,
                        {nameof(NullableIntClass.NullableInt)} integer null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void NullableInt()
        {
            using var con = fixture.OpenNewConnection();
            CreateNullableIntClassTable(con);

            NullableIntClass withNull = new NullableIntClass() { NullableInt = null };
            NullableIntClass with0 = new NullableIntClass() { NullableInt = 0 };
            NullableIntClass with1 = new NullableIntClass() { NullableInt = 1 };
            NullableIntClass withMinus1 = new NullableIntClass() { NullableInt = -1 };

            con.Insert(withNull);
            con.Insert(with0);
            con.Insert(with1);
            con.Insert(withMinus1);

            var results = con.Table<NullableIntClass>().OrderBy(x => x.ID).ToArray();

            Assert.Equal(4, results.Length);
            Assert.Equal(withNull.NullableInt, results[0].NullableInt);
            Assert.Equal(with0.NullableInt, results[1].NullableInt);
            Assert.Equal(with1.NullableInt, results[2].NullableInt);
            Assert.Equal(withMinus1.NullableInt, results[3].NullableInt);
        }


        public class NullableFloatClass
        {
            [PrimaryKey, AutoIncrement]
            public int ID { get; set; }

            public float? NullableFloat { get; set; }

            public override bool Equals(object obj)
            {
                NullableFloatClass other = (NullableFloatClass)obj;
                return ID == other.ID && NullableFloat == other.NullableFloat;
            }

            public override int GetHashCode()
            {
                return ID.GetHashCode() + NullableFloat.GetHashCode();
            }
        }

        static void CreateNullableFloatClassTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(NullableFloatClass)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(NullableFloatClass)} (
                        {nameof(NullableFloatClass.ID)} integer primary key autoincrement,
                        {nameof(NullableFloatClass.NullableFloat)} float null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void NullableFloat()
        {
            using var con = fixture.OpenNewConnection();
            CreateNullableFloatClassTable(con);

            var withNull = new NullableFloatClass() { NullableFloat = null };
            var with0 = new NullableFloatClass() { NullableFloat = 0 };
            var with1 = new NullableFloatClass() { NullableFloat = 1 };
            var withMinus1 = new NullableFloatClass() { NullableFloat = -1 };

            con.Insert(withNull);
            con.Insert(with0);
            con.Insert(with1);
            con.Insert(withMinus1);

            NullableFloatClass[] results = con.Table<NullableFloatClass>().OrderBy(x => x.ID).ToArray();

            Assert.Equal(4, results.Length);

            Assert.Equal(withNull.NullableFloat, results[0].NullableFloat);
            Assert.Equal(with0.NullableFloat, results[1].NullableFloat);
            Assert.Equal(with1.NullableFloat, results[2].NullableFloat);
            Assert.Equal(withMinus1.NullableFloat, results[3].NullableFloat);
        }

        public class StringClass
        {
            [PrimaryKey, AutoIncrement]
            public int ID { get; set; }

            //Strings are allowed to be null by default
            public string StringData { get; set; }

            public override bool Equals(object obj)
            {
                var other = (StringClass)obj;
                return ID == other.ID && StringData == other.StringData;
            }

            public override int GetHashCode()
            {
                return ID.GetHashCode() + StringData.GetHashCode();
            }
        }

        static void CreateStringClassTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(StringClass)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(StringClass)} (
                        {nameof(StringClass.ID)} integer primary key autoincrement,
                        {nameof(StringClass.StringData)} nvarchar(250) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void NullableString()
        {
            using var con = fixture.OpenNewConnection();
            CreateStringClassTable(con);

            var withNull = new StringClass() { StringData = null };
            var withEmpty = new StringClass() { StringData = "" };
            var withData = new StringClass() { StringData = "data" };

            con.Insert(withNull);
            con.Insert(withEmpty);
            con.Insert(withData);

            var results = con.Table<StringClass>().OrderBy(x => x.ID).ToArray();

            Assert.Equal(3, results.Length);

            Assert.Equal(withNull, results[0]);
            Assert.Equal(withEmpty, results[1]);
            Assert.Equal(withData, results[2]);
        }

        [Fact]
        public void WhereNotNull()
        {
            using var con = fixture.OpenNewConnection();
            CreateNullableIntClassTable(con);

            var withNull = new NullableIntClass() { NullableInt = null };
            var with0 = new NullableIntClass() { NullableInt = 0 };
            var with1 = new NullableIntClass() { NullableInt = 1 };
            var withMinus1 = new NullableIntClass() { NullableInt = -1 };

            con.Insert(withNull);
            con.Insert(with0);
            con.Insert(with1);
            con.Insert(withMinus1);

            var results = con.Table<NullableIntClass>().Where(x => x.NullableInt != null).OrderBy(x => x.ID).ToArray();

            Assert.Equal(3, results.Length);

            Assert.Equal(with0, results[0]);
            Assert.Equal(with1, results[1]);
            Assert.Equal(withMinus1, results[2]);
        }

        [Fact]
        public void WhereNull()
        {
            using var con = fixture.OpenNewConnection();
            CreateNullableIntClassTable(con);

            var withNull = new NullableIntClass() { NullableInt = null };
            var with0 = new NullableIntClass() { NullableInt = 0 };
            var with1 = new NullableIntClass() { NullableInt = 1 };
            var withMinus1 = new NullableIntClass() { NullableInt = -1 };

            con.Insert(withNull);
            con.Insert(with0);
            con.Insert(with1);
            con.Insert(withMinus1);

            var results = con.Table<NullableIntClass>()
                .Where(x => x.NullableInt == null)
                .OrderBy(x => x.ID)
                .ToArray();

            Assert.Single(results);
            Assert.Equal(withNull, results[0]);
        }

        [Fact]
        public void StringWhereNull()
        {
            using var con = fixture.OpenNewConnection();
            CreateStringClassTable(con);

            var withNull = new StringClass() { StringData = null };
            var withEmpty = new StringClass() { StringData = "" };
            var withData = new StringClass() { StringData = "data" };

            con.Insert(withNull);
            con.Insert(withEmpty);
            con.Insert(withData);

            var results = con.Table<StringClass>()
                .Where(x => x.StringData == null)
                .OrderBy(x => x.ID)
                .ToArray();
            Assert.Single(results);
            Assert.Equal(withNull, results[0]);
        }

        [Fact]
        public void StringWhereNotNull()
        {
            using var con = fixture.OpenNewConnection();
            CreateStringClassTable(con);

            var withNull = new StringClass() { StringData = null };
            var withEmpty = new StringClass() { StringData = "" };
            var withData = new StringClass() { StringData = "data" };

            con.Insert(withNull);
            con.Insert(withEmpty);
            con.Insert(withData);

            var results = con.Table<StringClass>()
                .Where(x => x.StringData != null)
                .OrderBy(x => x.ID)
                .ToArray();
            Assert.Equal(2, results.Length);
            Assert.Equal(withEmpty, results[0]);
            Assert.Equal(withData, results[1]);
        }

        public enum TestIntEnum
        {
            One = 1,
            Two = 2,
        }

        [StoreAsText]
        public enum TestTextEnum
        {
            Alpha,
            Beta,
        }

        public class NullableEnumClass
        {
            [PrimaryKey, AutoIncrement]
            public int ID { get; set; }

            public TestIntEnum? NullableIntEnum { get; set; }

            public TestTextEnum? NullableTextEnum { get; set; }

            public override bool Equals(object obj)
            {
                var other = (NullableEnumClass)obj;
                return ID == other.ID &&
                    NullableIntEnum == other.NullableIntEnum &&
                    NullableTextEnum == other.NullableTextEnum;
            }

            public override int GetHashCode()
            {
                return ID.GetHashCode() +
                    NullableIntEnum.GetHashCode() +
                    NullableTextEnum.GetHashCode();
            }

            public override string ToString()
            {
                return string.Format("[NullableEnumClass: ID={0}, NullableIntEnum={1}, NullableTextEnum={2}]", ID, NullableIntEnum, NullableTextEnum);
            }
        }

        static void CreateNullableEnumClassTable(DbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    drop table if exists {nameof(NullableEnumClass)};";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(NullableEnumClass)} (
                        {nameof(NullableEnumClass.ID)} integer primary key autoincrement,
                        {nameof(NullableEnumClass.NullableIntEnum)} integer null,
                        {nameof(NullableEnumClass.NullableTextEnum)} nvarchar(250) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }


        [Fact]
        public void NullableEnum()
        {
            using var con = fixture.OpenNewConnection();
            CreateNullableEnumClassTable(con);

            var withNull = new NullableEnumClass { NullableIntEnum = null, NullableTextEnum = null };
            var with1 = new NullableEnumClass { NullableIntEnum = TestIntEnum.One, NullableTextEnum = null };
            var with2 = new NullableEnumClass { NullableIntEnum = TestIntEnum.Two, NullableTextEnum = null };
            var withNullA = new NullableEnumClass { NullableIntEnum = null, NullableTextEnum = TestTextEnum.Alpha };
            var with1B = new NullableEnumClass { NullableIntEnum = TestIntEnum.One, NullableTextEnum = TestTextEnum.Beta };

            con.Insert(withNull);
            con.Insert(with1);
            con.Insert(with2);
            con.Insert(withNullA);
            con.Insert(with1B);

            var results = con.Table<NullableEnumClass>().OrderBy(x => x.ID).ToArray();

            Assert.Equal(5, results.Length);

            Assert.Equal(withNull, results[0]);
            Assert.Equal(with1, results[1]);
            Assert.Equal(with2, results[2]);
            Assert.Equal(withNullA, results[3]);
            Assert.Equal(with1B, results[4]);
        }
    }
}
