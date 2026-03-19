using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Kuery.Tests.SqlClient
{
    public class SqlClientFixture : IDisposable
    {
        public string Database { get; }
        readonly string dataSource;
        readonly bool integratedSecurity;
        readonly string username;
        readonly string password;
        readonly string masterDatabase;

        public SqlConnection CreateConnection() =>
            CreateConnection(Database);

        private SqlConnection CreateConnection(string database)
        {
            var csb = new SqlConnectionStringBuilder();
            csb.DataSource = dataSource;
            csb.InitialCatalog = database;
            csb.TrustServerCertificate = true;
            csb.IntegratedSecurity = integratedSecurity;
            if (!integratedSecurity)
            {
                csb.UserID = username;
                csb.Password = password;
            }
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
            dataSource = ReadStringSetting("KUERY_TEST_SQLSERVER_HOST", "(local)");
            integratedSecurity = ReadBoolSetting("KUERY_TEST_SQLSERVER_INTEGRATED_SECURITY", true);
            username = ReadStringSetting("KUERY_TEST_SQLSERVER_USERNAME", "sa");
            password = ReadStringSetting("KUERY_TEST_SQLSERVER_PASSWORD", "Your_password123");
            masterDatabase = ReadStringSetting("KUERY_TEST_SQLSERVER_MASTER_DB", "master");

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
            using (var connection = CreateConnection(masterDatabase))
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
            using (var connection = CreateConnection(masterDatabase))
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

        private static string ReadStringSetting(string name, string defaultValue)
        {
            var value = Environment.GetEnvironmentVariable(name);
            return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
        }

        private static bool ReadBoolSetting(string name, bool defaultValue)
        {
            var value = Environment.GetEnvironmentVariable(name);
            if (bool.TryParse(value, out bool parsed))
            {
                return parsed;
            }

            return defaultValue;
        }
    }
}
