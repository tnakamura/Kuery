using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Kuery.Tests.SqlClient
{
    public class SqlClientFixture : IDisposable
    {
        public string Database { get; }

        public SqlConnection CreateConnection() =>
            CreateConnection(Database);

        private static SqlConnection CreateConnection(string database)
        {
            var csb = new SqlConnectionStringBuilder();
            csb.DataSource = "(local)";
            csb.InitialCatalog = database;
            csb.TrustServerCertificate = true;
            csb.IntegratedSecurity = true;
            return new SqlConnection(csb.ToString());
        }

        public async Task<SqlConnection> OpenNewConnectionAsync()
        {
            SqlConnection connection = null;
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

        public SqlConnection OpenNewConnection()
        {
            SqlConnection connection = null;
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
            using (var connection = CreateConnection("master"))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE DATABASE [{Database}]";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void DeleteDatabase()
        {
            using (var connection = CreateConnection("master"))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
DECLARE @SQL nvarchar(1000);
IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = N'{Database}')
BEGIN
    SET @SQL = N'USE [{Database}];
                 ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                 USE [master];
                 DROP DATABASE [{Database}];';
    EXEC (@SQL);
END;";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
