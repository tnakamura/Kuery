using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using System.Data.Common;

namespace Kuery.Tests
{
    public class ByteArrayTest : IClassFixture<SqlServerFixture>
    {
        readonly SqlServerFixture fixture;

        public ByteArrayTest(SqlServerFixture fixture)
        {
            this.fixture = fixture;
        }

        public class ByteArrayClass
        {
            [PrimaryKey, AutoIncrement]
            public int ID { get; set; }

            public byte[] bytes { get; set; }

            public void AssertEquals(ByteArrayClass other)
            {
                Assert.Equal(other.ID, ID);
                if (other.bytes == null || bytes == null)
                {
                    Assert.Null(other.bytes);
                    Assert.Null(bytes);
                }
                else
                {
                    Assert.Equal(other.bytes.Length, bytes.Length);
                    for (var i = 0; i < bytes.Length; i++)
                    {
                        Assert.Equal(other.bytes[i], bytes[i]);
                    }
                }
            }
        }

        void CreateTestTable(DbConnection connection)
        {
            connection.DropTable(nameof(ByteArrayClass));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    if object_id (N'{nameof(ByteArrayClass)}') is null
                        create table [{nameof(ByteArrayClass)}] (
                            {nameof(ByteArrayClass.ID)} integer identity(1,1) primary key not null,
                            {nameof(ByteArrayClass.bytes)} varbinary(max) null
                        );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void ByteArrays()
        {
            var byteArrays = new ByteArrayClass[]
            {
                new ByteArrayClass() { bytes = new byte[] { 1, 2, 3, 4, 250, 252, 253, 254, 255 } }, //Range check
				new ByteArrayClass() { bytes = new byte[] { 0 } }, //null bytes need to be handled correctly
				new ByteArrayClass() { bytes = new byte[] { 0, 0 } },
                new ByteArrayClass() { bytes = new byte[] { 0, 1, 0 } },
                new ByteArrayClass() { bytes = new byte[] { 1, 0, 1 } },
                new ByteArrayClass() { bytes = new byte[] { } }, //Empty byte array should stay empty (and not become null)
				new ByteArrayClass() { bytes = null } //Null should be supported
			};

            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            //Insert all of the ByteArrayClass
            foreach (ByteArrayClass b in byteArrays)
            {
                con.Insert(b);
            }

            //Get them back out
            ByteArrayClass[] fetchedByteArrays = con.Table<ByteArrayClass>()
                .OrderBy(x => x.ID).ToArray();

            Assert.Equal(fetchedByteArrays.Length, byteArrays.Length);
            //Check they are the same
            for (int i = 0; i < byteArrays.Length; i++)
            {
                byteArrays[i].AssertEquals(fetchedByteArrays[i]);
            }
        }

        [Fact]
        public void ByteArrayWhere()
        {
            //Byte Arrays for comparisson
            var byteArrays = new ByteArrayClass[] {
                new ByteArrayClass() { bytes = new byte[] { 1, 2, 3, 4, 250, 252, 253, 254, 255 } }, //Range check
				new ByteArrayClass() { bytes = new byte[] { 0 } }, //null bytes need to be handled correctly
				new ByteArrayClass() { bytes = new byte[] { 0, 0 } },
                new ByteArrayClass() { bytes = new byte[] { 0, 1, 0 } },
                new ByteArrayClass() { bytes = new byte[] { 1, 0, 1 } },
                new ByteArrayClass() { bytes = new byte[] { } }, //Empty byte array should stay empty (and not become null)
				new ByteArrayClass() { bytes = null } //Null should be supported
			};

            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            var criterion = new byte[] { 1, 0, 1 };

            //Insert all of the ByteArrayClass
            var id = 0;
            foreach (var b in byteArrays)
            {
                con.Insert(b);
                if (b.bytes != null && criterion.SequenceEqual<byte>(b.bytes))
                {
                    id = b.ID;
                }
            }
            Assert.NotEqual(0, id);

            //Get it back out
            ByteArrayClass fetchedByteArray = con.Table<ByteArrayClass>().Where(x => x.bytes == criterion).First();
            Assert.NotNull(fetchedByteArray);
            //Check they are the same
            Assert.Equal(id, fetchedByteArray.ID);
        }

        [Fact]
        public void ByteArrayWhereNull()
        {
            //Byte Arrays for comparisson
            var byteArrays = new ByteArrayClass[]
            {
                new ByteArrayClass() { bytes = new byte[] { 1, 2, 3, 4, 250, 252, 253, 254, 255 } }, //Range check
				new ByteArrayClass() { bytes = new byte[] { 0 } }, //null bytes need to be handled correctly
				new ByteArrayClass() { bytes = new byte[] { 0, 0 } },
                new ByteArrayClass() { bytes = new byte[] { 0, 1, 0 } },
                new ByteArrayClass() { bytes = new byte[] { 1, 0, 1 } },
                new ByteArrayClass() { bytes = new byte[] { } }, //Empty byte array should stay empty (and not become null)
				new ByteArrayClass() { bytes = null } //Null should be supported
			};

            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            byte[] criterion = null;

            //Insert all of the ByteArrayClass
            var id = 0;
            foreach (var b in byteArrays)
            {
                con.Insert(b);
                if (b.bytes == null)
                {
                    id = b.ID;
                }
            }
            Assert.NotEqual(0, id);

            //Get it back out
            var fetchedByteArray = con.Table<ByteArrayClass>()
                .Where(x => x.bytes == criterion).First();

            Assert.NotNull(fetchedByteArray);
            //Check they are the same
            Assert.Equal(id, fetchedByteArray.ID);
        }

        [Fact]
        public void LargeByteArray()
        {
            const int byteArraySize = 1024 * 1024;
            var bytes = new byte[byteArraySize];
            for (int i = 0; i < byteArraySize; i++)
            {
                bytes[i] = (byte)(i % 256);
            }

            var byteArray = new ByteArrayClass() { bytes = bytes };

            using var con = fixture.OpenNewConnection();
            CreateTestTable(con);

            //Insert the ByteArrayClass
            con.Insert(byteArray);

            //Get it back out
            var fetchedByteArrays = con.Table<ByteArrayClass>().ToArray();

            Assert.Single(fetchedByteArrays);

            //Check they are the same
            byteArray.AssertEquals(fetchedByteArrays[0]);
        }
    }
}
