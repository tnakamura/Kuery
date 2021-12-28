using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests
{
    public class AsyncTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public AsyncTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public class AsyncCustomer
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }

            [MaxLength(64)]
            public string FirstName { get; set; }

            [MaxLength(64)]
            public string LastName { get; set; }

            [MaxLength(64), Indexed]
            public string Email { get; set; }
        }

        void CreateTable(DbConnection connection)
        {
            connection.DropTable(nameof(AsyncCustomer));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(AsyncCustomer)} (
                        {nameof(AsyncCustomer.Id)} integer primary key autoincrement,
                        {nameof(AsyncCustomer.FirstName)} nvarchar(64) not null,
                        {nameof(AsyncCustomer.LastName)} nvarchar(64) null,
                        {nameof(AsyncCustomer.Email)} nvarchar(64) null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public async Task QueryAsync()
        {
            using var connection = fixture.OpenNewConnection();
            CreateTable(connection);

            var customer = new AsyncCustomer
            {
                FirstName = "Joe"
            };

            await connection.InsertAsync(customer);

            await connection.QueryAsync<AsyncCustomer>("select * from AsyncCustomer");
        }

        AsyncCustomer CreateCustomer()
        {
            AsyncCustomer customer = new AsyncCustomer()
            {
                FirstName = "foo",
                LastName = "bar",
                Email = Guid.NewGuid().ToString()
            };
            return customer;
        }

        [Fact]
        public void TestInsertAsync()
        {
            // create...
            AsyncCustomer customer = CreateCustomer();

            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // run...
            conn.InsertAsync(customer).Wait();

            // check that we got an id...
            Assert.NotEqual(0, customer.Id);

            // check...
            using (var check = fixture.OpenNewConnection())
            {
                // load it back...
                var loaded = check.Get<AsyncCustomer>(customer.Id);
                Assert.Equal(loaded.Id, customer.Id);
            }
        }

        [Fact]
        public void TestUpdateAsync()
        {
            // create...
            AsyncCustomer customer = CreateCustomer();

            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // run...
            conn.InsertAsync(customer).Wait();

            // change it...
            string newEmail = Guid.NewGuid().ToString();
            customer.Email = newEmail;

            // save it...
            conn.UpdateAsync(customer).Wait();

            // check...
            using (var check = fixture.OpenNewConnection())
            {
                // load it back - should be changed...
                var loaded = check.Get<AsyncCustomer>(customer.Id);
                Assert.Equal(newEmail, loaded.Email);
            }
        }

        [Fact]
        public void TestDeleteAsync()
        {
            // create...
            AsyncCustomer customer = CreateCustomer();

            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // run...
            conn.InsertAsync(customer).Wait();

            // delete it...
            conn.DeleteAsync(customer).Wait();

            // check...
            using (var check = fixture.OpenNewConnection())
            {
                // load it back - should be null...
                var loaded = check.Table<AsyncCustomer>().Where(v => v.Id == customer.Id).ToList();
                Assert.Empty(loaded);
            }
        }

        [Fact]
        public void GetAsync()
        {
            // create...
            AsyncCustomer customer = new AsyncCustomer();
            customer.FirstName = "foo";
            customer.LastName = "bar";
            customer.Email = Guid.NewGuid().ToString();

            // connect and insert...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.InsertAsync(customer).Wait();

            // check...
            Assert.NotEqual(0, customer.Id);

            // get it back...
            var task = conn.GetAsync<AsyncCustomer>(customer.Id);
            task.Wait();
            AsyncCustomer loaded = task.Result;

            // check...
            Assert.Equal(customer.Id, loaded.Id);
        }

        [Fact]
        public void FindAsyncWithExpression()
        {
            // create...
            AsyncCustomer customer = new AsyncCustomer();
            customer.FirstName = "foo";
            customer.LastName = "bar";
            customer.Email = Guid.NewGuid().ToString();

            // connect and insert...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.InsertAsync(customer).Wait();

            // check...
            Assert.NotEqual(0, customer.Id);

            // get it back...
            var task = conn.FindAsync<AsyncCustomer>(x => x.Id == customer.Id);
            task.Wait();
            AsyncCustomer loaded = task.Result;

            // check...
            Assert.Equal(customer.Id, loaded.Id);
        }

        [Fact]
        public void FindAsyncWithExpressionNull()
        {
            // connect and insert...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // get it back...
            var task = conn.FindAsync<AsyncCustomer>(x => x.Id == 1);
            task.Wait();
            var loaded = task.Result;

            // check...
            Assert.Null(loaded);
        }

        [Fact]
        public void TestFindAsyncItemPresent()
        {
            // create...
            var customer = CreateCustomer();

            // connect and insert...
            var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.InsertAsync(customer).Wait();

            // check...
            Assert.NotEqual(0, customer.Id);

            // get it back...
            var task = conn.FindAsync<AsyncCustomer>(customer.Id);
            task.Wait();
            AsyncCustomer loaded = task.Result;

            // check...
            Assert.Equal(customer.Id, loaded.Id);
        }

        [Fact]
        public void TestFindAsyncItemMissing()
        {
            // connect and insert...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // now get one that doesn't exist...
            var task = conn.FindAsync<AsyncCustomer>(-1);
            task.Wait();

            // check...
            Assert.Null(task.Result);
        }

        [Fact]
        public void TestQueryAsync()
        {
            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // insert some...
            List<AsyncCustomer> customers = new List<AsyncCustomer>();
            for (int index = 0; index < 5; index++)
            {
                AsyncCustomer customer = CreateCustomer();

                // insert...
                conn.InsertAsync(customer).Wait();

                // add...
                customers.Add(customer);
            }

            // return the third one...
            var task = conn.QueryAsync<AsyncCustomer>(
                "select * from AsyncCustomer where id=$Id",
                new
                {
                    Id = customers[2].Id,
                });
            task.Wait();
            var loaded = task.Result.ToList();

            // check...
            Assert.Single(loaded);
            Assert.Equal(customers[2].Email, loaded[0].Email);
        }

        [Fact]
        public void TestTableAsync()
        {
            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync("delete from AsyncCustomer").Wait();

            // insert some...
            var customers = new List<AsyncCustomer>();
            for (int index = 0; index < 5; index++)
            {
                var customer = new AsyncCustomer();
                customer.FirstName = "foo";
                customer.LastName = "bar";
                customer.Email = Guid.NewGuid().ToString();

                // insert...
                conn.InsertAsync(customer).Wait();

                // add...
                customers.Add(customer);
            }

            // run the table operation...
            var query = conn.Table<AsyncCustomer>();
            var loaded = query.ToListAsync().Result;

            // check that we got them all back...
            Assert.Equal(5, loaded.Count);
            Assert.NotNull(loaded.Where(v => v.Id == customers[0].Id));
            Assert.NotNull(loaded.Where(v => v.Id == customers[1].Id));
            Assert.NotNull(loaded.Where(v => v.Id == customers[2].Id));
            Assert.NotNull(loaded.Where(v => v.Id == customers[3].Id));
            Assert.NotNull(loaded.Where(v => v.Id == customers[4].Id));
        }

        [Fact]
        public void TestExecuteAsync()
        {
            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // do a manual insert...
            var email = Guid.NewGuid().ToString();
            conn.ExecuteAsync(
                @"insert into AsyncCustomer (firstname, lastname, email)
                  values ($firstname, $lastname, $email)",
                new
                {
                    firstname = "foo",
                    lastname = "bar",
                    email,
                }).Wait();

            // check...
            using (var check = fixture.OpenNewConnection())
            {
                // load it back - should be null...
                var result = check.Table<AsyncCustomer>().Where(v => v.Email == email);
                Assert.NotNull(result);
            }
        }

        [Fact]
        public void TestInsertAllAsync()
        {
            // create a bunch of customers...
            var customers = new List<AsyncCustomer>();
            for (int index = 0; index < 100; index++)
            {
                AsyncCustomer customer = new AsyncCustomer();
                customer.FirstName = "foo";
                customer.LastName = "bar";
                customer.Email = Guid.NewGuid().ToString();
                customers.Add(customer);
            }

            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // insert them all...
            conn.InsertAllAsync(customers).Wait();

            // check...
            using (var check = fixture.OpenNewConnection())
            {
                for (int index = 0; index < customers.Count; index++)
                {
                    // load it back and check...
                    AsyncCustomer loaded = check.Get<AsyncCustomer>(customers[index].Id);
                    Assert.Equal(loaded.Email, customers[index].Email);
                }
            }
        }

        [Fact]
        public void TestExecuteScalar()
        {
            // connect...
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // check...
            var task = conn.ExecuteScalarAsync<object>(
                $"select name from sqlite_master where type='table' and name='{nameof(AsyncCustomer)}'");
            task.Wait();
            object name = task.Result;
            Assert.Equal(nameof(AsyncCustomer), name);
        }

        [Fact]
        public void TestAsyncTableQueryToListAsync()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // create...
            var customer = this.CreateCustomer();
            conn.InsertAsync(customer).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>();
            var task = query.ToListAsync();
            task.Wait();
            var items = task.Result;

            // check...
            var loaded = items.Where(v => v.Id == customer.Id).First();
            Assert.Equal(customer.Email, loaded.Email);
        }

        [Fact]
        public void TestAsyncTableQueryToFirstAsyncFound()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // create...
            var customer = this.CreateCustomer();
            conn.InsertAsync(customer).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>().Where(v => v.Id == customer.Id);
            var task = query.FirstAsync();
            task.Wait();
            var loaded = task.Result;

            // check...
            Assert.Equal(customer.Email, loaded.Email);
        }

        [Fact]
        public void TestAsyncTableQueryToFirstAsyncMissing()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // create...
            var customer = this.CreateCustomer();
            conn.InsertAsync(customer).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>().Where(v => v.Id == -1);
            var task = query.FirstAsync();
            Assert.Throws<AggregateException>(() => task.Wait());
        }

        [Fact]
        public void TestAsyncTableQueryToFirstOrDefaultAsyncFound()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // create...
            AsyncCustomer customer = this.CreateCustomer();
            conn.InsertAsync(customer).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>().Where(v => v.Id == customer.Id);
            var task = query.FirstOrDefaultAsync();
            task.Wait();
            var loaded = task.Result;

            // check...
            Assert.Equal(customer.Email, loaded.Email);
        }

        [Fact]
        public void TestAsyncTableQueryToFirstOrDefaultAsyncMissing()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // create...
            var customer = this.CreateCustomer();
            conn.InsertAsync(customer).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>().Where(v => v.Id == -1);
            var task = query.FirstOrDefaultAsync();
            task.Wait();
            var loaded = task.Result;

            // check...
            Assert.Null(loaded);
        }

        [Fact]
        public void TestAsyncTableQueryWhereOperation()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);

            // create...
            AsyncCustomer customer = this.CreateCustomer();
            conn.InsertAsync(customer).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>();
            var task = query.ToListAsync();
            task.Wait();
            var items = task.Result;

            // check...
            var loaded = items.Where(v => v.Id == customer.Id).First();
            Assert.Equal(customer.Email, loaded.Email);
        }

        [Fact]
        public void TestAsyncTableQueryCountAsync()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
                conn.InsertAsync(this.CreateCustomer()).Wait();

            // load...
            var query = conn.Table<AsyncCustomer>();
            var task = query.CountAsync();
            task.Wait();

            // check...
            Assert.Equal(10, task.Result);
        }

        [Fact]
        public void TestAsyncTableOrderBy()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
                conn.InsertAsync(this.CreateCustomer()).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>().OrderBy(v => v.Email);
            var task = query.ToListAsync();
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();
            task.Wait();
            var items = task.Result;

            // check...
            Assert.Equal(-1, string.Compare(items[0].Email, items[9].Email));
        }

        [Fact]
        public void TestAsyncTableOrderByDescending()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
                conn.InsertAsync(this.CreateCustomer()).Wait();

            // query...
            var query = conn.Table<AsyncCustomer>().OrderByDescending(v => v.Email);
            var task = query.ToListAsync();
            task.Wait();
            var items = task.Result;

            // check...
            Assert.Equal(1, string.Compare(items[0].Email, items[9].Email));
        }

        [Fact]
        public void TestAsyncTableQueryTake()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
            {
                var customer = this.CreateCustomer();
                customer.FirstName = index.ToString();
                conn.InsertAsync(customer).Wait();
            }

            // query...
            var query = conn.Table<AsyncCustomer>().OrderBy(v => v.FirstName).Take(1);
            var task = query.ToListAsync();
            task.Wait();
            var items = task.Result;

            // check...
            Assert.Single(items);
            Assert.Equal("0", items[0].FirstName);
        }

        [Fact]
        public void TestAsyncTableQuerySkip()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
            {
                var customer = this.CreateCustomer();
                customer.FirstName = index.ToString();
                conn.InsertAsync(customer).Wait();
            }

            // query...
            var query = conn.Table<AsyncCustomer>().OrderBy(v => v.FirstName).Skip(5);
            var task = query.ToListAsync();
            task.Wait();
            var items = task.Result;

            // check...
            Assert.Equal(5, items.Count);
            Assert.Equal("5", items[0].FirstName);
        }

        [Fact]
        public void TestAsyncTableElementAtAsync()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
            {
                var customer = this.CreateCustomer();
                customer.FirstName = index.ToString();
                conn.InsertAsync(customer).Wait();
            }

            // query...
            var query = conn.Table<AsyncCustomer>().OrderBy(v => v.FirstName);
            var task = query.ElementAtAsync(7);
            task.Wait();
            var loaded = task.Result;

            // check...
            Assert.Equal("7", loaded.FirstName);
        }


        [Fact]
        public void TestAsyncGetWithExpression()
        {
            using var conn = fixture.OpenNewConnection();
            CreateTable(conn);
            conn.ExecuteAsync($"delete from {nameof(AsyncCustomer)}").Wait();

            // create...
            for (int index = 0; index < 10; index++)
            {
                var customer = this.CreateCustomer();
                customer.FirstName = index.ToString();
                conn.InsertAsync(customer).Wait();
            }

            // get...
            var result = conn.GetAsync<AsyncCustomer>(x => x.FirstName == "7");
            result.Wait();
            var loaded = result.Result;
            // check...
            Assert.Equal("7", loaded.FirstName);
        }
    }
}
