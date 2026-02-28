namespace Kuery.Linq
{
    internal enum SetOperationKind
    {
        Union,
        UnionAll,
        Intersect,
        Except,
    }

    internal sealed class SetOperationQueryModel
    {
        internal SetOperationQueryModel(
            object left,
            object right,
            SetOperationKind operation)
        {
            Left = left ?? throw new System.ArgumentNullException(nameof(left));
            Right = right ?? throw new System.ArgumentNullException(nameof(right));
            Operation = operation;
        }

        internal object Left { get; }

        internal object Right { get; }

        internal SetOperationKind Operation { get; }

        internal TableMapping Table
        {
            get
            {
                if (Left is SelectQueryModel sq) return sq.Table;
                if (Left is SetOperationQueryModel so) return so.Table;
                throw new System.InvalidOperationException("Unexpected left model type.");
            }
        }
    }
}
