using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Kuery.Tests.SqlClient
{
    public class SqlClientFixture : IDisposable
    {
        public string Database { get; }

        public DbConnection CreateConnection() =>
            CreateConnection(Database);

        private static DbConnection CreateConnection(string database)
        {
            var csb = new SqlConnectionStringBuilder();
            csb.DataSource = "(local)";
            csb.InitialCatalog = database;
            csb.IntegratedSecurity = true;
            return new SqlConnection(csb.ToString());
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

        public SqlClientFixture()
        {
            Database = $"kuery_test_{Guid.NewGuid():N}";

            DeleteDatabase();

            CreateDatabase();

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
            DeleteDatabase();
        }

        private void CreateDatabase()
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE DATABASE [{Database}] IF NOT EXISTS";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void DeleteDatabase()
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"DROP DATABASE [{Database}] IF EXISTS";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
