using System;
using System.Collections.Generic;

namespace Kuery.Linq
{
    internal sealed class GeneratedSql
    {
        internal GeneratedSql(string commandText, IReadOnlyList<object> parameters)
        {
            CommandText = commandText ?? throw new ArgumentNullException(nameof(commandText));
            Parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        internal string CommandText { get; }

        internal IReadOnlyList<object> Parameters { get; }
    }
}
