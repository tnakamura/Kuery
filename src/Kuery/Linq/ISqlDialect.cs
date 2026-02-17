namespace Kuery.Linq
{
    internal enum SqlDialectKind
    {
        Sqlite,
        SqlServer,
        PostgreSql,
    }

    internal interface ISqlDialect
    {
        SqlDialectKind Kind { get; }

        string EscapeIdentifier(string identifier);

        string FormatParameterName(string parameterName);
    }
}
