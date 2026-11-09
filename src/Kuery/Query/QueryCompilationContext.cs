using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.Internal;

namespace Kuery.Query
{
    // Coordinates translation and shaper compilation, following EF Core's compilation stages.
    internal sealed class QueryCompilationContext
    {
        readonly QueryTranslationPreprocessor _preprocessor = new QueryTranslationPreprocessor();

        internal CompiledQuery CreateQueryExecutor(Expression query)
        {
            var preprocessed = _preprocessor.Process(query);
            var translated = new RelationalQueryableMethodTranslatingExpressionVisitor().Translate(preprocessed);
            return new RelationalShapedQueryCompilingExpressionVisitor().Compile(translated);
        }

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
