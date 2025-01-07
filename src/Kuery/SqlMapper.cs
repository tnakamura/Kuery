using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery
{
    internal static class SqlMapper
    {
        private static readonly Dictionary<Type, TableMapping> _s_mappings = new Dictionary<Type, TableMapping>();

        internal static TableMapping GetMapping<T>(CreateFlags createFlags = CreateFlags.None) =>
            GetMapping(typeof(T), createFlags);

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
