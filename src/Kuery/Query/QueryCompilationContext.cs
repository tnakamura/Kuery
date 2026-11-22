using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.Internal;

namespace Kuery.Query
{
    // Coordinates translation and shaper compilation, following EF Core's compilation stages.
    /// <summary>Coordinates expression preprocessing, relational translation, and shaper compilation.</summary>
    internal sealed class QueryCompilationContext
    {
        readonly QueryTranslationPreprocessor _preprocessor = new QueryTranslationPreprocessor();

        /// <summary>Preprocesses and translates a read query, then compiles its materializer.</summary>
        /// <param name="query">The query expression or SELECT to process.</param>
        /// <returns>The translated query and compiled result delegates.</returns>
        internal CompiledQuery CreateQueryExecutor(Expression query)
        {
            var preprocessed = _preprocessor.Process(query);
            var translated = new RelationalQueryableMethodTranslatingExpressionVisitor().TranslateRoot(preprocessed);
            return new RelationalShapedQueryCompilingExpressionVisitor().Compile(translated);
        }

        /// <summary>Preprocesses and translates a bulk update or delete.</summary>
        /// <param name="query">The query expression or SELECT to process.</param>
        /// <param name="operation">The command kind, such as select, update, or delete.</param>
        /// <param name="setters">The property assignments for an update, if applicable.</param>
        /// <returns>The compiled plan for the mutation.</returns>
        internal CompiledQuery CreateNonQueryExecutor(Expression query, string operation, IReadOnlyList<SetPropertyCall> setters)
        {
            var preprocessed = _preprocessor.Process(query);
            var translator = new RelationalQueryableMethodTranslatingExpressionVisitor();
            return new CompiledQuery
            {
                NonQuery = operation == "delete" ? translator.TranslateExecuteDelete(preprocessed) : translator.TranslateExecuteUpdate(preprocessed, setters),
            };
        }
    }
}
