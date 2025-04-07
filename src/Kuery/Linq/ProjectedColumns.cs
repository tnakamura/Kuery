using System.Collections.ObjectModel;
using System.Linq.Expressions;
using Kuery.Linq.Expressions;

namespace Kuery.Linq
{
    internal sealed class ProjectedColumns
    {
        internal ProjectedColumns(
            Expression projector,
            ReadOnlyCollection<ColumnDeclaration> columns)
        {
            Projector = projector;
            Columns = columns;
        }

        internal Expression Projector { get; }

        internal ReadOnlyCollection<ColumnDeclaration> Columns { get; }
    }
}
