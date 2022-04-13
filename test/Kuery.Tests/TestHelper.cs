#nullable enable
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace Kuery.Tests
{
    internal static class TestHelper
    {
        public static void DropTable(this SqlConnection connection, string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                    drop table if exists [{tableName}];";
                command.ExecuteNonQuery();
            }
        }

        public static void DropTable(this SqliteConnection connection, string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                    drop table if exists [{tableName}];";
                command.ExecuteNonQuery();
            }
        }
    }
}
