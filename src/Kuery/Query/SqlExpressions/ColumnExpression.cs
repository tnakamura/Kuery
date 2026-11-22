using System;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>References a column through the alias of its table source.</summary>
    internal sealed class ColumnExpression : SqlExpression
    {
        /// <summary>Creates a reference to a column in an aliased table source.</summary>
        /// <param name="alias">The alias of the table source that supplies the column.</param>
        /// <param name="name">The mapped column name or projected alias.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="nullable">Whether the SQL value can be NULL.</param>
        internal ColumnExpression(string alias, string name, Type type, bool nullable)
            : base(type, nullable) { TableAlias = alias; Name = name; }
        /// <summary>Gets the alias of the table or derived table that owns the column.</summary>
        internal string TableAlias { get; }
        /// <summary>Gets the database column name or projected output alias.</summary>
        internal string Name { get; }
    }
}
