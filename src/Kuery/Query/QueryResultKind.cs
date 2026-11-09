using System;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal enum QueryResultKind { Sequence, First, FirstOrDefault, Single, SingleOrDefault, ElementAt, ElementAtOrDefault, Scalar }
}
