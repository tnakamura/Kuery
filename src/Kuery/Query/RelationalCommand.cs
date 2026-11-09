using System;
using System.Collections.Generic;
using System.Globalization;

namespace Kuery.Query
{
    internal sealed class BoundParameter
    {
        internal string Name;
        internal object Value;
        internal Type Type;
        internal Type InputType;
        internal int SourceIndex;

        internal static object ConvertValue(object value, Type type)
        {
            var target = Nullable.GetUnderlyingType(type) ?? type;
            if (!target.IsEnum || value == null) return value;
            return target.IsDefined(typeof(StoreAsTextAttribute), true) ? value.ToString()
                : Convert.ChangeType(value, Enum.GetUnderlyingType(target), CultureInfo.InvariantCulture);
        }
    }

    internal sealed class RelationalCommand
    {
        internal string CommandText;
        internal readonly List<BoundParameter> Parameters = new List<BoundParameter>();
    }

}
