using System;
using System.Threading.Tasks;

namespace Kuery.Tests.Npgsql
{
    public class NpgsqlFixture : IDisposable
    {
        public string Database { get; }

        readonly string host;
        readonly int port;
        readonly string username;
        readonly string password;
        readonly string masterDatabase;

        public global::Npgsql.NpgsqlConnection CreateConnection() =>
            CreateConnection(Database);

        private global::Npgsql.NpgsqlConnection CreateConnection(string database)
        {
            var csb = new global::Npgsql.NpgsqlConnectionStringBuilder();
            csb.Username = username;
            csb.Password = password;
            csb.Host = host;
            csb.Port = port;
            csb.Database = database;
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
            host = ReadStringSetting("KUERY_TEST_PG_HOST", "localhost");
            port = ReadIntSetting("KUERY_TEST_PG_PORT", 5432);
            username = ReadStringSetting("KUERY_TEST_PG_USERNAME", "postgres");
            password = ReadStringSetting("KUERY_TEST_PG_PASSWORD", "postgres");
            masterDatabase = ReadStringSetting("KUERY_TEST_PG_MASTER_DB", "postgres");

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
                ExecuteNonQuery(connection, "UPDATE pg_database SET datallowconn = 'false' WHERE datname = @databaseName", "databaseName", Database);
                ExecuteNonQuery(connection, $"ALTER DATABASE {databaseIdentifier} CONNECTION LIMIT 1");
                ExecuteNonQuery(connection, $@"
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE datname = @databaseName", "databaseName", Database);
                ExecuteNonQuery(connection, $"DROP DATABASE IF EXISTS {databaseIdentifier}");
            }
        }

        private static void ExecuteNonQuery(global::Npgsql.NpgsqlConnection connection, string commandText)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private static void ExecuteNonQuery(global::Npgsql.NpgsqlConnection connection, string commandText, string parameterName, object parameterValue)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = commandText;
                if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
                var normalizedParameterName = parameterName.StartsWith("@", StringComparison.Ordinal) ? parameterName : "@" + parameterName;
                command.Parameters.AddWithValue(normalizedParameterName, parameterValue ?? DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        private static string QuoteIdentifier(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            foreach (var c in name)
            {
                if (!char.IsLetterOrDigit(c) && c != '_')
                {
                    throw new ArgumentException("Database name contains unsupported characters.", nameof(name));
                }
            }
            return $"\"{name}\"";
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
