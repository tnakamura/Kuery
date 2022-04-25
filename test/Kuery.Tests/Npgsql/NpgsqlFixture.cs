using System;
using System.Threading.Tasks;

namespace Kuery.Tests.Npgsql
{
    public class NpgsqlFixture : IDisposable
    {
        public string Database { get; }

        public global::Npgsql.NpgsqlConnection CreateConnection() =>
            CreateConnection(Database);

        private static global::Npgsql.NpgsqlConnection CreateConnection(string database)
        {
            var csb = new global::Npgsql.NpgsqlConnectionStringBuilder();
            csb.TrustServerCertificate = true;
            return new global::Npgsql.NpgsqlConnection(csb.ToString());
        }

        public async Task<global::Npgsql.NpgsqlConnection> OpenNewConnectionAsync()
        {
            global::Npgsql.NpgsqlConnection connection = null;
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

        public global::Npgsql.NpgsqlConnection OpenNewConnection()
        {
            global::Npgsql.NpgsqlConnection connection = null;
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

        public NpgsqlFixture()
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
                            code NVARCHAR(50) NOT NULL,
                            name NVARCHAR(100) NOT NULL
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
