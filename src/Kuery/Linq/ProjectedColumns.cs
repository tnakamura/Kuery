using System.Collections.ObjectModel;
using System.Linq.Expressions;
using Kuery.Linq.Expressions;

namespace Kuery.Linq
{
    internal sealed class ProjectedColumns
    {
        internal ProjectedColumns(
            Expression projection,
            ReadOnlyCollection<ColumnDeclaration> columns)
        {
            Projection = projection;
            Columns = columns;
        }

        internal Expression Projection { get; }

        internal ReadOnlyCollection<ColumnDeclaration> Columns { get; }
    }
}
