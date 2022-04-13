using System;
using System.Data.Common;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Data.Sqlite;

namespace Kuery.Tests.Sqlite
{
    public class SqliteFixture : IDisposable
    {
        public string DataSource { get; }

        public DbConnection CreateConnection() =>
            CreateConnection(DataSource);

        private static DbConnection CreateConnection(string dataSource)
        {
            var csb = new SqliteConnectionStringBuilder();
            csb.DataSource = dataSource;
            return new SqliteConnection(csb.ToString());
        }

        public async Task<DbConnection> OpenNewConnectionAsync()
        {
            DbConnection connection = null;
            try
            {
                connection = CreateConnection();
                await connection.OpenAsync();
                return connection;
            }
            catch
            {
                connection?.Close();
                throw;
            }
        }

        public DbConnection OpenNewConnection()
        {
            DbConnection connection = null;
            try
            {
                connection = CreateConnection();
                connection.Open();
                return connection;
            }
            catch
            {
                connection?.Close();
                throw;
            }
        }

        public SqliteFixture()
        {
            var dbName = $"kuery_test_{Guid.NewGuid():N}";
            DataSource = Path.Combine(
                AppContext.BaseDirectory,
                $"{dbName}.sqlite3");

            DeleteDataSource();

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"CREATE TABLE customers (
                            id INTEGER NOT NULL PRIMARY KEY,
                            code TEXT NOT NULL,
                            name TEXT NOT NULL
                          )";
                    command.ExecuteNonQuery();
                }
            }
        }

        public void Dispose()
        {
            DeleteDataSource();
        }

        private void DeleteDataSource()
        {
            try
            {
                if (File.Exists(DataSource))
                {
                    File.Delete(DataSource);
                }
            }
            catch (IOException)
            {
            }
        }
    }
}
