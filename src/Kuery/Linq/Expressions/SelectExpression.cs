using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace Kuery.Linq.Expressions
{
    internal class SelectExpression : Expression
    {
        internal SelectExpression(
            Type type,
            string alias,
            IEnumerable<ColumnDeclaration> columns,
            Expression from,
            Expression where)
            : base()
        {
            NodeType = (ExpressionType)DbExpressionType.Select;
            Type = type;
            Alias = Alias;
            From = from;
            Where = where;
            Columns = columns as ReadOnlyCollection<ColumnDeclaration>;
            if (Columns == null)
            {
                Columns = new List<ColumnDeclaration>(columns).AsReadOnly();
            }
        }

        /// <inheritdoc/>
        public override ExpressionType NodeType { get; }

        /// <inheritdoc/>
        public override Type Type { get; }

        internal string Alias { get; }

        internal ReadOnlyCollection<ColumnDeclaration> Columns { get; }

        internal Expression From { get; }

        internal Expression Where { get; }
    }
}
