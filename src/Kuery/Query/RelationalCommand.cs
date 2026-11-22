using System;
using System.Collections.Generic;
using System.Globalization;

namespace Kuery.Query
{
    /// <summary>Describes a generated SQL parameter and its value or runtime binding slot.</summary>
    internal sealed class BoundParameter
    {
        /// <summary>The provider-specific name used in the SQL text and command.</summary>
        internal string Name;
        /// <summary>The converted parameter value bound for the current execution.</summary>
        internal object Value;
        /// <summary>The CLR type used to choose the database parameter type.</summary>
        internal Type Type;
        /// <summary>The CLR type before converting a runtime value for provider binding.</summary>
        internal Type InputType;
        /// <summary>The index of the runtime value slot, or a negative value when the binding has no slot.</summary>
        internal int SourceIndex;

        /// <summary>Converts enum values to their mapped name or underlying numeric representation.</summary>
        /// <param name="value">The value used by the operation.</param>
        /// <param name="type">The CLR type of the value before provider conversion.</param>
        /// <returns>The value to send to the database provider.</returns>
        internal static object ConvertValue(object value, Type type)
        {
            var target = Nullable.GetUnderlyingType(type) ?? type;
            if (!target.IsEnum || value == null) return value;
            return target.IsDefined(typeof(StoreAsTextAttribute), true) ? value.ToString()
                : Convert.ChangeType(value, Enum.GetUnderlyingType(target), CultureInfo.InvariantCulture);
        }
    }

    /// <summary>Pairs generated SQL command text with its parameter bindings.</summary>
    internal sealed class RelationalCommand
    {
        /// <summary>The generated SQL to execute.</summary>
        internal string CommandText;
        /// <summary>The ordered list of parameter bindings referenced by the SQL.</summary>
        internal readonly List<BoundParameter> Parameters = new List<BoundParameter>();
    }

}
