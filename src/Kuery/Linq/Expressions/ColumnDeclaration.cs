using System.Linq.Expressions;

namespace Kuery.Linq.Expressions
{
    internal class ColumnDeclaration
    {
        internal ColumnDeclaration(string name, Expression expression)
        {
            Name = name;
            Expression = expression;
        }

        internal string Name { get; }

        internal Expression Expression { get; }
    }
}
