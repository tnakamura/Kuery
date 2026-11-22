using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Provides CLR type and nullability metadata for scalar SQL expressions.</summary>
    internal abstract class SqlExpression : Expression
    {
        /// <summary>Initializes a SQL expression with its CLR type and nullability.</summary>
        /// <param name="type">The CLR type of the SQL value.</param>
        /// <param name="nullable">Whether the SQL expression can evaluate to NULL.</param>
        protected SqlExpression(Type type, bool nullable)
        {
            Type = type;
            IsNullable = nullable;
        }

        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public sealed override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Gets the CLR type of the value represented by this SQL expression.</summary>
        public sealed override Type Type { get; }
        /// <summary>Gets whether the SQL expression can evaluate to NULL.</summary>
        internal bool IsNullable { get; }
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
