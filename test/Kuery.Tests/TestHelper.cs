#nullable enable
using System.Data.Common;

namespace Kuery.Tests
{
    internal static class TestHelper
    {
        public static void DropTable(this DbConnection connection, string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                    if object_id (N'{tableName}') is not null
                        drop table [{tableName}];";
                command.ExecuteNonQuery();
            }
        }
    }
}
