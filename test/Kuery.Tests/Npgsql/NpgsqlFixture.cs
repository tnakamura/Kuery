using System;
using System.Threading.Tasks;

namespace Kuery.Tests.Npgsql
{
    public class NpgsqlFixture : IDisposable
    {
        public string Database { get; }

        const string MasterDatabase = "postgres";

        public global::Npgsql.NpgsqlConnection CreateConnection() =>
            CreateConnection(Database);

        private static global::Npgsql.NpgsqlConnection CreateConnection(string database)
        {
            var csb = new global::Npgsql.NpgsqlConnectionStringBuilder();
            csb.Username = "postgres";
            csb.Password = "postgres";
            csb.Host = "localhost";
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
            using (var connection = CreateConnection(MasterDatabase))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE DATABASE \"{Database}\"";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void DeleteDatabase()
        {
            using (var connection = CreateConnection(MasterDatabase))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
UPDATE pg_database SET datallowconn = 'false' WHERE datname = '{Database}';
ALTER DATABASE ""{Database}"" CONNECTION LIMIT 1;

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE datname = '{Database}';

DROP DATABASE IF EXISTS ""{Database}"";";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
