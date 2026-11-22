using System;
using System.Data;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    /// <summary>Stores the translated SQL tree and result delegates shared across executions of a query shape.</summary>
    internal sealed class CompiledQuery
    {
        /// <summary>Gets or sets the translated read query and its CLR result shape.</summary>
        internal ShapedQueryExpression ShapedQuery { get; set; }
        /// <summary>Gets or sets the translated update or delete operation.</summary>
        internal NonQueryExpression NonQuery { get; set; }
        /// <summary>Gets the SELECT tree underlying the read or mutation operation.</summary>
        internal SelectExpression Query => ShapedQuery?.QueryExpression ?? NonQuery.QueryExpression;
        /// <summary>Gets how a read operation interprets the returned rows.</summary>
        internal QueryResultKind ResultKind => ShapedQuery.ResultKind;
        /// <summary>Gets the CLR type constructed by the read query.</summary>
        internal Type ElementType => ShapedQuery.ShaperExpression.Type;
        /// <summary>Gets or sets the delegate that materializes one result from a data record.</summary>
        internal Func<IDataRecord, object> Materialize { get; set; }
        /// <summary>Gets or sets the plan for materializing a collection from multiple rows, if needed.</summary>
        internal CollectionQueryPlan Collection { get; set; }
        /// <summary>The cached SQL and parameter slot metadata without runtime parameter values.</summary>
        internal RelationalCommand CommandTemplate;
    }
}
