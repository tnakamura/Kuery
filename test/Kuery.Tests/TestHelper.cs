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
                    drop table if exists [{tableName}];";
                command.ExecuteNonQuery();
            }
        }
    }
}
