using System;
using System.Collections.Generic;
using System.Text;

namespace Kuery.Linq
{
    internal static class TypeSystem
    {
        internal static Type GetElementType(Type sequenceType)
        {
            var ienumerable = FindIEnumerable(sequenceType);
            if (ienumerable is null)
            {
                return sequenceType;
            }
            return ienumerable.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type sequenceType)
        {
            if (sequenceType is null || sequenceType == typeof(string))
            {
                return null;
            }

            if (sequenceType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(sequenceType.GetElementType());
            }

            if (sequenceType.IsGenericType)
            {
                foreach (var arg in sequenceType.GetGenericArguments())
                {
                    var ienumerable = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienumerable.IsAssignableFrom(sequenceType))
                    {
                        return ienumerable;
                    }
                }
            }

            var interfaceTypes = sequenceType.GetInterfaces();

            if (interfaceTypes != null && interfaceTypes.Length > 0)
            {
                foreach (var interfaceType in interfaceTypes)
                {
                    var ienumerable = FindIEnumerable(interfaceType);
                    if (ienumerable != null)
                    {
                        return ienumerable;
                    }
                }
            }

            if (sequenceType.BaseType != null && sequenceType.BaseType != typeof(object))
            {
                return FindIEnumerable(sequenceType.BaseType);
            }

            return null;
        }
    }
}
