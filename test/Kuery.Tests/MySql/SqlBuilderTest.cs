using System.Collections.Generic;
using System.Data;
using System.Linq;
using Kuery;
using Xunit;

namespace Kuery.Tests.MySql
{
    public class SqlBuilderTest : IClassFixture<MySqlFixture>
    {
        readonly MySqlFixture fixture;

        public SqlBuilderTest(MySqlFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void GetParameterPrefixReturnsAtForMySql()
        {
            using (var connection = fixture.CreateConnection())
            {
                var prefix = connection.GetParameterPrefix();

                Assert.Equal("@", prefix);
            }
        }

        [Fact]
        public void EscapeLiteralUsesBackticksForMySql()
        {
            using (var connection = fixture.CreateConnection())
            {
                var escaped = connection.EscapeLiteral("customers");

                Assert.Equal("`customers`", escaped);
            }
        }

        [Fact]
        public void CreateParameterizedCommandUsesAtPrefixedParametersForDictionary()
        {
            using (var connection = fixture.CreateConnection())
            using (var command = connection.CreateParameterizedCommand(
                "SELECT * FROM customers WHERE id = @id AND code = @code",
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

                Assert.Contains("@id", parameterNames);
                Assert.Contains("@code", parameterNames);
            }
        }
    }
}
