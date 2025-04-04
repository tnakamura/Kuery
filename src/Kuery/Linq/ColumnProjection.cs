using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal class ColumnProjection
    {
        internal string Columns;
        internal Expression Selector;
    }
}
