using System.Collections.Generic;
using System.Data;
using System.Linq;
using Kuery;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class SqlBuilderTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public SqlBuilderTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void GetParameterPrefixReturnsDollarForSqlite()
        {
            using (var connection = fixture.CreateConnection())
            {
                var prefix = connection.GetParameterPrefix();

                Assert.Equal("$", prefix);
            }
        }

        [Fact]
        public void EscapeLiteralUsesSquareBracketsForSqlite()
        {
            using (var connection = fixture.CreateConnection())
            {
                var escaped = connection.EscapeLiteral("customers");

                Assert.Equal("[customers]", escaped);
            }
        }

        [Fact]
        public void CreateParameterizedCommandUsesDollarPrefixedParametersForDictionary()
        {
            using (var connection = fixture.CreateConnection())
            using (var command = connection.CreateParameterizedCommand(
                "SELECT * FROM customers WHERE id = $id AND code = $code",
                new Dictionary<string, object>
                {
                    ["id"] = 1,
                    ["code"] = "A001",
                }))
            {
                var parameterNames = command.Parameters
                    .Cast<IDataParameter>()
                    .Select(x => x.ParameterName)
                    .ToArray();

                Assert.Contains("$id", parameterNames);
                Assert.Contains("$code", parameterNames);
            }
        }

        [Fact]
        public void CreateGetByPrimaryKeyCommandUsesSqliteParameterName()
        {
            using (var connection = fixture.CreateConnection())
            {
                var map = SqlMapper.GetMapping<Customer>();

                using (var command = connection.CreateGetByPrimaryKeyCommand(map, 2))
                {
                    var parameter = Assert.IsAssignableFrom<IDataParameter>(command.Parameters[0]);

                    Assert.Equal("$id", parameter.ParameterName);
                    Assert.Contains("[customers]", command.CommandText);
                    Assert.Contains("[id] = $id", command.CommandText);
                }
            }
        }
    }
}
