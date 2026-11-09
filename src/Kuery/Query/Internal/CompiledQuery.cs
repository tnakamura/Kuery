using System;
using System.Data;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal sealed class CompiledQuery
    {
        internal ShapedQueryExpression ShapedQuery { get; set; }
        internal NonQueryExpression NonQuery { get; set; }
        internal SelectExpression Query => ShapedQuery?.QueryExpression ?? NonQuery.QueryExpression;
        internal QueryResultKind ResultKind => ShapedQuery.ResultKind;
        internal Type ElementType => ShapedQuery.ShaperExpression.Type;
        internal Func<IDataRecord, object> Materialize { get; set; }
        internal RelationalCommand CommandTemplate;
    }
}
