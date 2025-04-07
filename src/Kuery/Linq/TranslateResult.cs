using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal class TranslateResult
    {
        internal string CommandText;
        internal LambdaExpression Projector;
    }
}
