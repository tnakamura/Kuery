using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Text;

namespace Kuery.Linq
{
    internal class ObjectReader<T> : IEnumerable<T>
        where T : class, new()
    {
        private Enumerator enumerator;

        internal ObjectReader(DbDataReader reader)
        {
            enumerator = new Enumerator(reader);
        }

        public IEnumerator<T> GetEnumerator()
        {
            var e = enumerator;
            if (e == null)
            {
                throw new InvalidOperationException(
                    "Cannot enumerate more than once");
            }
            enumerator = null;
            return e;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        class Enumerator : IEnumerator<T>
        {
            private DbDataReader reader;
            private FieldInfo[] fields;
            private int[] fieldLookup;
            private T current;

            internal Enumerator(DbDataReader reader)
            {
                this.reader = reader;
                fields = typeof(T).GetFields(
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            }

            /// <inheritdoc/>
            public T Current => current;

            /// <inheritdoc/>
            object IEnumerator.Current => current;

            /// <inheritdoc/>
            public bool MoveNext()
            {
                if (reader.Read())
                {
                    if (fieldLookup == null)
                    {
                        InitFieldLookup();
                    }

                    var instance = new T();

                    for (var i = 0; i < fields.Length; i++)
                    {
                        var index = fieldLookup[i];

                        if (index >= 0)
                        {
                            var fi = fields[i];

                            if (reader.IsDBNull(index))
                            {
                                fi.SetValue(instance, null);
                            }
                            else
                            {
                                fi.SetValue(instance, reader.GetValue(index));
                            }
                        }
                    }

                    current = instance;

                    return true;
                }
                return false;
            }

            /// <inheritdoc/>
            public void Reset()
            {
            }

            /// <inheritdoc/>
            public void Dispose() => reader.Dispose();

            private void InitFieldLookup()
            {
                var map = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    map.Add(reader.GetName(i), i);
                }

                fieldLookup = new int[fields.Length];

                for (var i = 0; i < fields.Length; i++)
                {
                    if (map.TryGetValue(fields[i].Name, out var index))
                    {
                        fieldLookup[i] = index;
                    }
                    else
                    {
                        fieldLookup[i] = -1;
                    }
                }
            }
        }
    }
}
