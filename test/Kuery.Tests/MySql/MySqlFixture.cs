using System;
using System.Threading.Tasks;
using MySqlConnector;

namespace Kuery.Tests.MySql
{
    public class MySqlFixture : IDisposable
    {
        public string Database { get; }

        readonly string host;
        readonly int port;
        readonly string username;
        readonly string password;
        readonly string masterDatabase;

        public MySqlConnection CreateConnection() =>
            CreateConnection(Database);

        private MySqlConnection CreateConnection(string database)
        {
            var csb = new MySqlConnectionStringBuilder();
            csb.Server = host;
            if (port < 0 || port > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Invalid MySQL port '{port}'. Expected a value between 0 and {ushort.MaxValue}.");
            }
            csb.Port = (uint)port;
            csb.UserID = username;
            csb.Password = password;
            csb.Database = database;
            return new MySqlConnection(csb.ToString());
        }

        public async Task<MySqlConnection> OpenNewConnectionAsync()
        {
            MySqlConnection connection = null;
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

        public MySqlConnection OpenNewConnection()
        {
            MySqlConnection connection = null;
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

        public MySqlFixture()
        {
            host = ReadStringSetting("KUERY_TEST_MYSQL_HOST", "localhost");
            port = ReadIntSetting("KUERY_TEST_MYSQL_PORT", 33060);
            username = ReadStringSetting("KUERY_TEST_MYSQL_USERNAME", "root");
            password = ReadStringSetting("KUERY_TEST_MYSQL_PASSWORD", "mysql");
            masterDatabase = ReadStringSetting("KUERY_TEST_MYSQL_MASTER_DB", "mysql");

            Database = $"kuery_test_{Guid.NewGuid():N}";

            CreateDatabase();

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"CREATE TABLE IF NOT EXISTS customers (
                            id INTEGER NOT NULL PRIMARY KEY,
                            code VARCHAR(50) NOT NULL,
                            name VARCHAR(100) NOT NULL
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
            var databaseIdentifier = QuoteIdentifier(Database);
            using (var connection = CreateConnection(masterDatabase))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE DATABASE {databaseIdentifier}";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void DeleteDatabase()
        {
            var databaseIdentifier = QuoteIdentifier(Database);
            using (var connection = CreateConnection(masterDatabase))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"DROP DATABASE IF EXISTS {databaseIdentifier}";
                    command.ExecuteNonQuery();
                }
            }
        }

        private static string QuoteIdentifier(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            for (var i = 0; i < name.Length; i++)
            {
                var c = name[i];
                if (!char.IsLetterOrDigit(c) && c != '_')
                {
                    throw new ArgumentException($"Database name contains unsupported character '{c}' at index {i}.", nameof(name));
                }
            }
            return $"`{name}`";
        }

        private static string ReadStringSetting(string name, string defaultValue)
        {
            var value = Environment.GetEnvironmentVariable(name);
            return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
        }

        private static int ReadIntSetting(string name, int defaultValue)
        {
            var value = Environment.GetEnvironmentVariable(name);
            if (int.TryParse(value, out int parsed))
            {
                return parsed;
            }

            return defaultValue;
        }
    }
}
