using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery
{
    /// <summary>Caches table mappings and reads database records into mapped CLR objects.</summary>
    internal static class SqlMapper
    {
        private static readonly Dictionary<Type, TableMapping> _s_mappings = new Dictionary<Type, TableMapping>();

        /// <summary>Looks up or builds the table mapping for a CLR type and set of convention flags.</summary>
        /// <typeparam name="T">The CLR type whose table mapping is requested.</typeparam>
        /// <param name="createFlags">The flags that control implicit mapping conventions.</param>
        /// <returns>The table mapping used for commands and materialization.</returns>
        /// <remarks>Mappings are cached by CLR type. A different set of explicit convention flags can replace the cached mapping.</remarks>
        internal static TableMapping GetMapping<T>(CreateFlags createFlags = CreateFlags.None) =>
            GetMapping(typeof(T), createFlags);

        /// <summary>Looks up or builds the table mapping for a CLR type and set of convention flags.</summary>
        /// <param name="type">The mapped CLR type.</param>
        /// <param name="createFlags">The flags that control implicit mapping conventions.</param>
        /// <returns>The table mapping used for commands and materialization.</returns>
        /// <remarks>Mappings are cached by CLR type. A different set of explicit convention flags can replace the cached mapping.</remarks>
        internal static TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            var key = type;
            TableMapping map;
            lock (_s_mappings)
            {
                if (_s_mappings.TryGetValue(key, out map))
                {
                    if (createFlags != CreateFlags.None && createFlags != map.CreateFlags)
                    {
                        map = new TableMapping(
                            type: type,
                            createFlags: createFlags);
                        _s_mappings[key] = map;
                    }
                }
                else
                {
                    map = new TableMapping(
                        type: type,
                        createFlags: createFlags);
                    _s_mappings.Add(key, map);
                }
            }
            return map;
        }

        /// <summary>Reads the remaining records into a list of mapped values.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="reader">The result reader positioned before the next row to materialize.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <returns>The materialized rows.</returns>
        internal static List<T> ToList<T>(this IDataReader reader, TableMapping map)
        {
            var result = new List<T>();
            var deserializer = new Deserializer<T>(map, reader);
            while (reader.Read())
            {
                var obj = deserializer.Deserialize(reader);
                result.Add(obj);
            }
            return result;
        }

        /// <summary>Reads the remaining records asynchronously into a list of mapped values.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="reader">The result reader positioned before the next row to materialize.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the materialized rows.</returns>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal static async Task<List<T>> ToListAsync<T>(this DbDataReader reader, TableMapping map, CancellationToken cancellationToken)
        {
            var result = new List<T>();
            var deserializer = new Deserializer<T>(map, reader);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var obj = deserializer.Deserialize(reader);
                result.Add(obj);
            }
            return result;
        }

        /// <summary>Reads and materializes the next record, if present.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="reader">The result reader positioned before the next row to materialize.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <returns>The mapped value, or the default value if no record is available.</returns>
        internal static T FirstOrDefault<T>(this IDataReader reader, TableMapping map)
        {
            var deserializer = new Deserializer<T>(map, reader);
            if (reader.Read())
            {
                return deserializer.Deserialize(reader);
            }
            else
            {
                return default;
            }
        }

        /// <summary>Reads and materializes the next record asynchronously, if present.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="reader">The result reader positioned before the next row to materialize.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the mapped value, or the default value if no record is available.</returns>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal static async Task<T> FirstOrDefaultAsync<T>(this DbDataReader reader, TableMapping map, CancellationToken cancellationToken)
        {
            var deserializer = new Deserializer<T>(map, reader);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return deserializer.Deserialize(reader);
            }
            else
            {
                return default;
            }
        }

        private readonly struct Deserializer<T>
        {
            private readonly TableMapping _map;

            private readonly MethodInfo _getSetter;

            private readonly TableMapping.Column[] _columns;

            private readonly Action<object, IDataRecord, int>[] _fastSetters;

            /// <summary>Builds an ordinal-to-property map for the reader's result columns.</summary>
            /// <param name="map">The mapping used to match result columns to CLR properties.</param>
            /// <param name="reader">The reader that supplies the result column names and ordinals.</param>
            public Deserializer(TableMapping map, IDataReader reader)
            {
                _map = map;

                if (typeof(T) != map.MappedType)
                {
                    _getSetter = typeof(FastSetter)
                        .GetMethod(
                            nameof(FastSetter.GetFastSetter),
                            BindingFlags.NonPublic | BindingFlags.Static)
                        .MakeGenericMethod(map.MappedType);
                }
                else
                {
                    _getSetter = null;
                }

                _columns = new TableMapping.Column[reader.FieldCount];
                _fastSetters = new Action<object, IDataRecord, int>[reader.FieldCount];
                for (var i = 0; i < _columns.Length; i++)
                {
                    var name = reader.GetName(i);
                    _columns[i] = map.FindColumn(name);
                    if (_columns[i] != null)
                    {
                        if (_getSetter != null)
                        {
                            _fastSetters[i] = (Action<object, IDataRecord, int>)_getSetter.Invoke(
                                null,
                                new object[] { _columns[i] });
                        }
                        else
                        {
                            //_fastSetters[i] = FastColumnSetter.GetFastSetter<T>(_columns[i]);
                            _fastSetters[i] = _columns[i].FastSetter;
                        }
                    }
                }
            }

            /// <summary>Creates and populates a mapped object from the current record.</summary>
            /// <param name="record">The positioned record containing the values to deserialize.</param>
            /// <returns>The populated entity cast to the requested result type.</returns>
            public T Deserialize(IDataRecord record)
            {
                var obj = Activator.CreateInstance(_map.MappedType);
                for (var i = 0; i < _columns.Length; i++)
                {
                    if (_columns[i] == null)
                    {
                        continue;
                    }

                    if (_fastSetters[i] != null)
                    {
                        _fastSetters[i].Invoke(obj, record, i);
                    }
                    else
                    {
                        var col = _columns[i];
                        var val = record.GetValue(i);
                        col.SetValue(obj, val);
                    }
                }
                return (T)obj;
            }
        }
    }
}
