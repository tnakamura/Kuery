using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Identifies the database dialect used during SQL generation.</summary>
    internal enum SqlDialectKind
    {
        /// <summary>The SQLite dialect.</summary>
        Sqlite,
        /// <summary>The Microsoft SQL Server dialect.</summary>
        SqlServer,
        /// <summary>The PostgreSQL dialect.</summary>
        PostgreSql,
        /// <summary>The MySQL dialect.</summary>
        MySql,
    }

    /// <summary>Defines provider-specific rules for SQL identifiers, parameters, casts, and functions.</summary>
    internal interface ISqlDialect
    {
        /// <summary>Gets the database dialect that these SQL rules target.</summary>
        SqlDialectKind Kind { get; }

        /// <summary>Quotes a database identifier and escapes embedded identifier delimiters.</summary>
        /// <param name="identifier">The unquoted identifier.</param>
        /// <returns>The identifier quoted for the current dialect.</returns>
        string EscapeIdentifier(string identifier);

        /// <summary>Prefixes a parameter name as required by the database provider.</summary>
        /// <param name="parameterName">The parameter name without its provider prefix.</param>
        /// <returns>The provider-specific parameter name.</returns>
        string FormatParameterName(string parameterName);

        /// <summary>Gets the SQL type name used to cast a value to the specified CLR type.</summary>
        /// <param name="type">The target CLR type of the cast.</param>
        /// <returns>The SQL type name for the cast.</returns>
        string CastType(System.Type type);

        /// <summary>Renders a supported database function or equivalent SQL expression.</summary>
        /// <param name="operation">The logical SQL operation name.</param>
        /// <param name="arguments">The rendered SQL arguments in call order.</param>
        /// <returns>The SQL fragment that implements the operation.</returns>
        string Function(string operation, string[] arguments);
    }
}
