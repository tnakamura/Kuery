using System;
using System.Linq.Expressions;

namespace Kuery.Linq.Expressions
{
    internal class ProjectionExpression : Expression
    {
        internal ProjectionExpression(SelectExpression source, Expression projector)
            : base()
        {
            NodeType = (ExpressionType)DbExpressionType.Projection;
            Type = projector.Type;
            Source = source;
            Projector = projector;
        }

        /// <inheritdoc/>
        public override ExpressionType NodeType { get; }

        /// <inheritdoc/>
        public override Type Type { get; }

        internal SelectExpression Source { get; }

        internal Expression Projector { get; }
    }
}
