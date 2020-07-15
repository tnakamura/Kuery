using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Kuery.Tests
{
    public class SqlServerFixture
    {
        public string DbName { get; }

        public DbConnection CreateConnection() =>
            CreateConnection(DbName);

        DbConnection CreateConnection(string database)
        {
            var csb = new SqlConnectionStringBuilder();
            csb.InitialCatalog = database;
            csb.IntegratedSecurity = true;
            csb.DataSource = "(local)\\SQLEXPRESS";
            return new SqlConnection(csb.ToString());
        }

        public SqlServerFixture()
        {
            DbName = "kuery_test";

            using (var connection = CreateConnection("master"))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                       $@"IF DB_ID (N'{DbName}') IS NOT NULL
                            DROP DATABASE [{DbName}]";
                    command.ExecuteNonQuery();
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"IF DB_ID (N'{DbName}') IS NULL
                            CREATE DATABASE [{DbName}]";
                    command.ExecuteNonQuery();
                }
            }

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"IF OBJECT_ID (N'dbo.customers') IS NULL
                            CREATE TABLE customers (
                              id INTEGER PRIMARY KEY NOT NULL,
                              code NVARCHAR(50) NOT NULL,
                              name NVARCHAR(50) NOT NULL
                            )";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
