using System;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kuery
{
    public static partial class SqlHelper
    {
        public static TableQuery<T> Table<T>(this IDbConnection connection)
        {
            var map = GetMapping<T>();
            return new TableQuery<T>(connection, map);
        }

        public static int Insert<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            return connection.Insert(typeof(T), item, transaction);
        }

        public static int Insert(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            var map = GetMapping(type);
            if (map.PK != null &&
                map.PK.IsAutoGuid &&
                (Guid)map.PK.GetValue(item) == Guid.Empty)
            {
                map.PK.SetValue(item, Guid.NewGuid());
            }

            int count;
            using (var command = connection.CreateInsertCommand(item, type))
            {
                command.Transaction = transaction;
                count = command.ExecuteNonQuery();
            }

            if (map.HasAutoIncPK)
            {
                var id = connection.GetLastRowId(transaction);
                map.SetAutoIncPk(item, id);
            }

            return count;
        }

        private static long GetLastRowId(this IDbConnection connection, IDbTransaction transaction = null)
        {
            using (var command = connection.CreateLastInsertRowIdCommand())
            {
                command.Transaction = transaction;
                var result = command.ExecuteScalar();
                if (result is decimal d)
                {
                    return (long)d;
                }
                return (long)command.ExecuteScalar();
            }
        }

        public static int InsertAll(this IDbConnection connection, IEnumerable items, IDbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += connection.Insert(Orm.GetType(item), item, transaction);
            }
            return result;
        }

        public static int InsertAll(this IDbConnection connection, Type type, IEnumerable items, IDbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (type == null) throw new ArgumentNullException(nameof(type));

            var result = 0;
            foreach (var item in items)
            {
                result += connection.Insert(type, item, transaction);
            }
            return result;
        }


        public static int Update<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return connection.Update(typeof(T), item, transaction);
        }

        public static int Update(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }

        public static int UpdateAll(this IDbConnection connection, IEnumerable items, IDbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateUpdateCommand(item, Orm.GetType(item)))
                {
                    if (transaction != null)
                    {
                        command.Transaction = transaction;
                    }
                    result += command.ExecuteNonQuery();
                }
            }
            return result;
        }

        public static int InsertOrReplace(this IDbConnection connection, object item, IDbTransaction transaction = null)
        {
            if (item == null)
            {
                return 0;
            }
            return connection.InsertOrReplace(Orm.GetType(item), item, transaction);
        }

        public static int InsertOrReplace(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (item == null)
            {
                return 0;
            }
            using (var command = connection.CreateInsertOrReplaceCommand(item, type))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }

        public static int Delete<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }

        public static int Delete<T>(this IDbConnection connection, object primaryKey, IDbTransaction transaction = null)
        {
            var map = GetMapping<T>();
            return connection.Delete(map, primaryKey, transaction);
        }

        public static int Delete(this IDbConnection connection, Type type, object primaryKey, IDbTransaction transaction = null)
        {
            var map = GetMapping(type);
            return connection.Delete(map, primaryKey, transaction);
        }

        private static int Delete(this IDbConnection connection, TableMapping map, object primaryKey, IDbTransaction transaction = null)
        {
            if (map == null) throw new ArgumentNullException(nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }


        public static T Find<T>(this IDbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.Find<T>(map, pk);
        }

        private static T Find<T>(this IDbConnection connection, TableMapping mapping, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                return ExecuteQueryFirstOrDefault<T>(command, mapping);
            }
        }

        public static T Find<T>(this IDbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstOrDefault(predicate);
        }

        public static T Get<T>(this IDbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().First(predicate);
        }

        public static T Get<T>(this IDbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.Get<T>(map, pk);
        }

        public static T Get<T>(this IDbConnection connection, Type type, object pk)
        {
            var map = GetMapping(type);
            return connection.Get<T>(map, pk);
        }

        private static T Get<T>(this IDbConnection connection, TableMapping mapping, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = ExecuteQuery<T>(command, mapping);
                return result.First();
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

        internal static List<T> ExecuteQuery<T>(this IDbCommand command, TableMapping map)
        {
            using (var reader = command.ExecuteReader())
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
        }

        internal static T ExecuteQueryFirstOrDefault<T>(this IDbCommand command, TableMapping map)
        {
            using (var reader = command.ExecuteReader())
            {
                var deserializer = new Deserializer<T>(map, reader);
                if (reader.Read())
                {
                    return deserializer.Deserialize(reader);
                }
                else
                {
                    return default(T);
                }
            }
        }

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

        public static IEnumerable<T> Query<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = GetMapping<T>();
                return ExecuteQuery<T>(command, map);
            }
        }

        public static IEnumerable<object> Query(this IDbConnection connection, Type type, string sql, object param = null)
        {
            var map = GetMapping(type);
            return connection.Query(map, sql, param);
        }

        private static IEnumerable<object> Query(this IDbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQuery<object>(command, mapping);
            }
        }

        public static T FindWithQuery<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = GetMapping<T>();
                return ExecuteQueryFirstOrDefault<T>(command, map);
            }
        }

        public static object FindWithQuery(this IDbConnection connection, Type type, string sql, object param = null)
        {
            var map = GetMapping(type);
            return connection.FindWithQuery(map, sql, param);
        }

        private static object FindWithQuery(this IDbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefault<object>(command, mapping);
            }
        }

        public static int Execute(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return command.ExecuteNonQuery();
            }
        }

        public static T ExecuteScalar<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = command.ExecuteScalar();
                if (result is null || result is DBNull)
                {
                    return default;
                }
                var clrType = typeof(T);
                if (clrType.IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    clrType = typeof(T).GetGenericArguments()[0];
                }
                return (T)Convert.ChangeType(result, clrType);
            }
        }
    }

    [AttributeUsage(AttributeTargets.Class)]
    public sealed class TableAttribute : Attribute
    {
        public string Name { get; }

        public bool WithoutRowId { get; set; }

        public TableAttribute(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ColumnAttribute : Attribute
    {
        public string Name { get; }

        public ColumnAttribute(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class PrimaryKeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class AutoIncrementAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class IndexedAttribute : Attribute
    {
        public string Name { get; set; }

        public int Order { get; set; }

        public virtual bool Unique { get; set; }

        public IndexedAttribute()
        {
        }

        public IndexedAttribute(string name, int order)
        {
            Name = name;
            Order = order;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class IgnoreAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class UniqueAttribute : IndexedAttribute
    {
        public override bool Unique
        {
            get => true;
            set { }
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MaxLengthAttribute : Attribute
    {
        public int Value { get; }

        public MaxLengthAttribute(int length)
        {
            Value = length;
        }
    }

    public sealed class PreserveAttribute : Attribute
    {
        public bool AllMembers { get; set; }

        public bool Conditional { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class CollationAttribute : Attribute
    {
        public string Value { get; }

        public CollationAttribute(string collation)
        {
            Value = collation;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class NotNullAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Enum)]
    public sealed class StoreAsTextAttribute : Attribute
    {
    }

    internal sealed class TableMapping
    {
        public Type MappedType { get; }

        public string TableName { get; }

        public bool WithoutRowId { get; }

        public IReadOnlyList<Column> Columns { get; }

        public Column PK { get; }

        public CreateFlags CreateFlags { get; }

        private readonly Column _autoPk;

        private readonly IReadOnlyDictionary<string, Column> _lookupByColumnName;

        private readonly IReadOnlyDictionary<string, Column> _lookupByPropertyName;

        internal TableMapping(
            Type type,
            CreateFlags createFlags = CreateFlags.None)
        {
            MappedType = type;
            CreateFlags = createFlags;

            var typeInfo = type.GetTypeInfo();
            var tableAttr = typeInfo.CustomAttributes
                .Where(x => x.AttributeType == typeof(TableAttribute))
                .Select(x => (TableAttribute)Orm.InflateAttribute(x))
                .FirstOrDefault();

            TableName = (tableAttr != null && !string.IsNullOrEmpty(tableAttr.Name)) ?
                tableAttr.Name :
                MappedType.Name;
            WithoutRowId = tableAttr != null ? tableAttr.WithoutRowId : false;

            var props = new List<PropertyInfo>();
            var baseType = type;
            var propNames = new HashSet<string>();
            while (baseType != typeof(object))
            {
                var ti = baseType.GetTypeInfo();
                var newProps = (
                    from p in ti.DeclaredProperties
                    where !propNames.Contains(p.Name)
                    where p.CanRead
                    where p.CanWrite
                    where p.GetMethod != null
                    where p.SetMethod != null
                    where p.GetMethod.IsPublic
                    where p.SetMethod.IsPublic
                    where !p.GetMethod.IsStatic
                    where !p.SetMethod.IsStatic
                    select p
                ).ToList();
                foreach (var p in newProps)
                {
                    propNames.Add(p.Name);
                }
                props.AddRange(newProps);
                baseType = ti.BaseType;
            }

            var cols = new List<Column>();
            foreach (var p in props)
            {
                var ignore = p.IsDefined(typeof(IgnoreAttribute), true);
                if (!ignore)
                {
                    cols.Add(new Column(MappedType, p, createFlags));
                }
            }
            Columns = cols.ToArray();
            foreach (var c in Columns)
            {
                if (c.IsAutoInc && c.IsPK)
                {
                    _autoPk = c;
                }
                if (c.IsPK)
                {
                    PK = c;
                }
            }

            _lookupByColumnName = Columns.ToDictionary(x => x.Name.ToLower());
            _lookupByPropertyName = Columns.ToDictionary(x => x.PropertyName);

            HasAutoIncPK = _autoPk != null;

            InsertColumns = Columns.Where(c => !c.IsAutoInc).ToArray();
            InsertOrReplaceColumns = Columns.ToArray();
        }

        public bool HasAutoIncPK { get; }

        public void SetAutoIncPk(object obj, long id)
        {
            if (_autoPk != null)
            {
                _autoPk.SetValue(obj, Convert.ChangeType(id, _autoPk.ColumnType, null));
            }
        }

        public IReadOnlyList<Column> InsertColumns { get; }

        public IReadOnlyList<Column> InsertOrReplaceColumns { get; }

        public Column FindColumnWithPropertyName(string propertyName) =>
            _lookupByPropertyName.TryGetValue(propertyName, out var column) ? column : null;

        public Column FindColumn(string columnName) =>
            _lookupByColumnName.TryGetValue(columnName.ToLower(), out var column) ? column : null;

        internal sealed class Column
        {
            private readonly PropertyInfo _prop;

            public string Name { get; }

            public PropertyInfo PropertyInfo => _prop;

            public string PropertyName => _prop.Name;

            public Type ColumnType { get; }

            public string Collation { get; }

            public bool IsAutoInc { get; }

            public bool IsAutoGuid { get; }

            public bool IsPK { get; }

            public IEnumerable<IndexedAttribute> Indices { get; }

            public bool IsNullable { get; }

            public int? MaxStringLength { get; }

            public bool StoreAsText { get; }

            internal readonly Type MappedType;

            internal readonly Action<object, IDataRecord, int> FastSetter;

            public Column(Type mappedType, PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
            {
                var colAttr = prop.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(ColumnAttribute));

                _prop = prop;
                MappedType = mappedType;
                Name = (colAttr != null && colAttr.ConstructorArguments.Count > 0) ?
                    colAttr.ConstructorArguments[0].Value?.ToString() :
                    prop.Name;

                ColumnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                Collation = Orm.Collation(prop);

                IsPK = Orm.IsPK(prop) ||
                    (((createFlags & CreateFlags.ImplicitPK) == CreateFlags.ImplicitPK) &&
                    string.Compare(prop.Name, Orm.ImplicitPkName, StringComparison.OrdinalIgnoreCase) == 0);

                var isAuto = Orm.IsAutoInc(prop) ||
                    (IsPK && ((createFlags & CreateFlags.AutoIncPK) == CreateFlags.AutoIncPK));
                IsAutoGuid = isAuto && ColumnType == typeof(Guid);
                IsAutoInc = isAuto && !IsAutoGuid;

                Indices = Orm.GetIndices(prop);
                if (!Indices.Any() &&
                    !IsPK &&
                    (createFlags & CreateFlags.ImplicitIndex) == CreateFlags.ImplicitIndex &&
                    Name.EndsWith(Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase))
                {
                    Indices = new IndexedAttribute[]
                    {
                        new IndexedAttribute()
                    };
                }
                IsNullable = !(IsPK || Orm.IsMarkedNotNull(prop));
                MaxStringLength = Orm.MaxStringLength(prop);

                StoreAsText = ColumnType
                    .GetTypeInfo()
                    .CustomAttributes
                    .Any(x => x.AttributeType == typeof(StoreAsTextAttribute));

                FastSetter = Kuery.FastSetter.Typeless.GetFastSetter(this);
            }

            public void SetValue(object obj, object val)
            {
                if (val == null || val == DBNull.Value)
                {
                    _prop.SetValue(
                        obj: obj,
                        value: null,
                        index: null);
                }
                else if (ColumnType.GetTypeInfo().IsEnum)
                {
                    if (val is string enumStr && StoreAsText)
                    {
                        _prop.SetValue(
                            obj: obj,
                            value: Enum.Parse(ColumnType, enumStr),
                            index: null);
                    }
                    else
                    {
                        _prop.SetValue(
                            obj: obj,
                            value: Enum.ToObject(ColumnType, val),
                            index: null);
                    }
                }
                else if ((_prop.PropertyType == typeof(TimeSpan) || _prop.PropertyType == typeof(TimeSpan?)) &&
                    val is string timeSpanStr &&
                    TimeSpan.TryParse(timeSpanStr, out var timespan))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: timespan,
                        index: null);
                }
                else if ((_prop.PropertyType == typeof(DateTime) || _prop.PropertyType == typeof(DateTime?)) &&
                    val is string dateTimeStr &&
                    DateTime.TryParse(dateTimeStr, out var dateTime))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: dateTime,
                        index: null);
                }
                else if ((_prop.PropertyType == typeof(DateTimeOffset) || _prop.PropertyType == typeof(DateTimeOffset?)) &&
                    val is string dateTimeOffsetStr &&
                    DateTimeOffset.TryParse(dateTimeOffsetStr, out var dateTimeOffset))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: dateTimeOffset,
                        index: null);
                }
                else if ((_prop.PropertyType == typeof(Guid) || _prop.PropertyType == typeof(Guid?)) &&
                    val is string guidStr &&
                    Guid.TryParse(guidStr, out var guid))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: guid,
                        index: null);
                }
                else if (_prop.PropertyType.IsGenericType && _prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var clrType = _prop.PropertyType.GetGenericArguments()[0];
                    _prop.SetValue(
                        obj: obj,
                        value: Convert.ChangeType(val, clrType),
                        index: null);
                }
                else
                {
                    _prop.SetValue(
                        obj: obj,
                        value: Convert.ChangeType(val, _prop.PropertyType),
                        index: null);
                }
            }

            public object GetValue(object obj)
            {
                return _prop.GetValue(obj, null);
            }
        }
    }

    sealed class EnumCacheInfo
    {
        public EnumCacheInfo(Type type)
        {
            var typeInfo = type.GetTypeInfo();

            IsEnum = typeInfo.IsEnum;

            if (IsEnum)
            {
                StoreAsText = typeInfo.CustomAttributes
                    .Any(x => x.AttributeType == typeof(StoreAsTextAttribute));
                if (StoreAsText)
                {
                    EnumValues = new Dictionary<int, string>();
                    foreach (var e in Enum.GetValues(type))
                    {
                        EnumValues[Convert.ToInt32(e)] = e.ToString();
                    }
                }
            }
        }

        public bool IsEnum { get; }

        public bool StoreAsText { get; }

        public Dictionary<int, string> EnumValues { get; }
    }

    static class EnumCache
    {
        private static readonly Dictionary<Type, EnumCacheInfo> _cache = new Dictionary<Type, EnumCacheInfo>();

        public static EnumCacheInfo GetInfo<T>()
        {
            return GetInfo(typeof(T));
        }

        public static EnumCacheInfo GetInfo(Type type)
        {
            lock (_cache)
            {
                if (!_cache.TryGetValue(type, out var info))
                {
                    info = new EnumCacheInfo(type);
                    _cache[type] = info;
                }
                return info;
            }
        }
    }

    static class Orm
    {
        public const int DefaultMaxStringLength = 140;
        public const string ImplicitPkName = "Id";
        public const string ImplicitIndexSuffix = "Id";

        public static Type GetType(object obj)
        {
            if (obj == null)
            {
                return typeof(object);
            }
            var rt = obj as IReflectableType;
            if (rt != null)
            {
                return rt.GetTypeInfo().AsType();
            }
            return obj.GetType();
        }

        public static string SqlDecl(TableMapping.Column p, bool storeDateTimeAsTicks, bool storeTimeSpanAsTicks)
        {
            var decl = "\"" +
                p.Name +
                "\" " +
                SqlType(p, storeDateTimeAsTicks, storeTimeSpanAsTicks)
                + " ";
            if (p.IsPK)
            {
                decl += "primary key ";
            }
            if (p.IsAutoInc)
            {
                decl += "autoincrement ";
            }
            if (!p.IsNullable)
            {
                decl += "not null ";
            }
            if (!string.IsNullOrEmpty(p.Collation))
            {
                decl += "collate " + p.Collation + " ";
            }
            return decl;
        }

        public static string SqlType(TableMapping.Column p, bool storeDateTimeAsTicks, bool storeTimeSpanAsTicks)
        {
            var clrType = p.ColumnType;
            if (clrType == typeof(bool) ||
                clrType == typeof(byte) ||
                clrType == typeof(ushort) ||
                clrType == typeof(sbyte) ||
                clrType == typeof(short) ||
                clrType == typeof(int) ||
                clrType == typeof(uint) ||
                clrType == typeof(long))
            {
                return "integer";
            }
            else if (clrType == typeof(float) ||
                clrType == typeof(double) ||
                clrType == typeof(decimal))
            {
                return "float";
            }
            else if (clrType == typeof(string) ||
                clrType == typeof(StringBuilder) ||
                clrType == typeof(Uri) ||
                clrType == typeof(UriBuilder))
            {
                var len = p.MaxStringLength;
                if (len.HasValue)
                {
                    return "varchar(" + len.Value + ")";
                }
                else
                {
                    return "varchar";
                }
            }
            else if (clrType == typeof(TimeSpan))
            {
                return storeTimeSpanAsTicks ? "bigint" : "time";
            }
            else if (clrType == typeof(DateTime))
            {
                return storeDateTimeAsTicks ? "bigint" : "datetime";
            }
            else if (clrType == typeof(DateTimeOffset))
            {
                return "bigint";
            }
            else if (clrType.GetTypeInfo().IsEnum)
            {
                if (p.StoreAsText)
                {
                    return "varchar";
                }
                else
                {
                    return "integer";
                }
            }
            else if (clrType == typeof(byte[]))
            {
                return "blob";
            }
            else if (clrType == typeof(Guid))
            {
                return "varchar(36)";
            }
            else
            {
                throw new NotSupportedException(
                    $"Don't know about {clrType}");
            }
        }

        public static bool IsPK(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(PrimaryKeyAttribute));
        }

        public static string Collation(MemberInfo p)
        {
            return p.CustomAttributes
                .Where(x => x.AttributeType == typeof(CollationAttribute))
                .Select(x =>
                {
                    var args = x.ConstructorArguments;
                    return args.Count > 0 ? ((args[0].Value as string) ?? "") : "";
                })
                .FirstOrDefault() ?? "";
        }

        public static bool IsAutoInc(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(AutoIncrementAttribute));
        }

        public static FieldInfo GetField(TypeInfo t, string name)
        {
            var f = t.GetDeclaredField(name);
            if (f != null)
            {
                return f;
            }
            return GetField(t.BaseType.GetTypeInfo(), name);
        }

        public static PropertyInfo GetProperty(TypeInfo t, string name)
        {
            var f = t.GetDeclaredProperty(name);
            if (f != null)
            {
                return f;
            }
            return GetProperty(t.BaseType.GetTypeInfo(), name);
        }

        public static object InflateAttribute(CustomAttributeData x)
        {
            var atype = x.AttributeType;
            var typeInfo = atype.GetTypeInfo();
            var args = x.ConstructorArguments.Select(a => a.Value).ToArray();
            var r = Activator.CreateInstance(x.AttributeType, args);
            foreach (var arg in x.NamedArguments)
            {
                if (arg.IsField)
                {
                    GetField(typeInfo, arg.MemberName).SetValue(r, arg.TypedValue.Value);
                }
                else
                {
                    GetProperty(typeInfo, arg.MemberName).SetValue(r, arg.TypedValue.Value);
                }
            }
            return r;
        }

        public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo p)
        {
            var indexedInfo = typeof(IndexedAttribute).GetTypeInfo();
            return p.CustomAttributes
                .Where(x => indexedInfo.IsAssignableFrom(x.AttributeType.GetTypeInfo()))
                .Select(x => (IndexedAttribute)InflateAttribute(x));
        }

        public static int? MaxStringLength(PropertyInfo p)
        {
            var attr = p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(MaxLengthAttribute));
            if (attr != null)
            {
                var attrv = (MaxLengthAttribute)InflateAttribute(attr);
                return attrv.Value;
            }
            return null;
        }

        public static bool IsMarkedNotNull(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(NotNullAttribute));
        }
    }

    [Flags]
    internal enum CreateFlags
    {
        None = 0x000,
        ImplicitPK = 0x001,
        ImplicitIndex = 0x002,
        AllImplicit = 0x003,
        AutoIncPK = 0x004,
        FullTextSearch3 = 0x100,
        FullTextSearch4 = 0x200,
    }

    public abstract class BaseTableQuery
    {
        protected class Ordering
        {
            public string ColumnName { get; set; }

            public bool Ascending { get; set; }
        }
    }

    public sealed partial class TableQuery<T> : BaseTableQuery, IEnumerable<T>
    {
        public IDbConnection Connection { get; }

        internal TableMapping Table { get; }

        private Expression _where;

        private List<Ordering> _orderBys;

        private int? _limit;

        private int? _offset;

        private BaseTableQuery _joinInner;

        private Expression _joinInnerKeySelector;

        private BaseTableQuery _joinOuter;

        private Expression _joinOuterKeySelector;

        private Expression _joinSelector;

        private Expression _selector;

        private bool _deferred;

        internal TableQuery(IDbConnection connection, TableMapping table)
        {
            Connection = connection;
            Table = table;
        }

        internal TableQuery(IDbConnection connection)
        {
            Connection = connection;
            Table = SqlHelper.GetMapping(typeof(T));
        }

        private TableQuery<TOther> Clone<TOther>()
        {
            var q = new TableQuery<TOther>(Connection, Table);
            q._where = _where;
            q._deferred = _deferred;
            if (_orderBys != null)
            {
                q._orderBys = new List<Ordering>(_orderBys);
            }
            q._limit = _limit;
            q._offset = _offset;
            q._joinInner = _joinInner;
            q._joinInnerKeySelector = _joinInnerKeySelector;
            q._joinOuter = _joinOuter;
            q._joinOuterKeySelector = _joinOuterKeySelector;
            q._joinSelector = _joinSelector;
            q._selector = _selector;
            return q;
        }

        public TableQuery<T> Where(Expression<Func<T, bool>> predExpr)
        {
            if (predExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)predExpr;
                var pred = lambda.Body;
                var q = Clone<T>();
                q.AddWhere(pred);
                return q;
            }
            else
            {
                throw new NotSupportedException("Must be a predicate");
            }
        }

        public int Delete()
        {
            return Delete(null);
        }

        public int Delete(Expression<Func<T, bool>> predExpr)
        {
            if (_limit.HasValue || _offset.HasValue)
            {
                throw new InvalidOperationException("Cannot delete with limits or offsets");
            }
            if (_where == null && predExpr == null)
            {
                throw new InvalidOperationException("No condition specified");
            }

            var pred = _where;

            if (predExpr != null && predExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)predExpr;
                pred = pred != null ? Expression.AndAlso(pred, lambda.Body) : lambda.Body;
            }

            var args = new List<object>();
            var cmdText = "delete from " + EscapeLiteral(Table.TableName);
            var w = CompileExpr(pred, args);
            cmdText += " where " + w.CommandText;

            using (var command = Connection.CreateCommand())
            {
                command.CommandText = cmdText;
                for (var i = 0; i < args.Count; i++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = Connection.GetParameterName("p" + (i + 1).ToString());
                    parameter.Value = args[i];
                    command.Parameters.Add(parameter);
                }
                var result = command.ExecuteNonQuery();
                return result;
            }
        }

        public TableQuery<T> Take(int n)
        {
            var q = Clone<T>();
            q._limit = n;
            return q;
        }

        public TableQuery<T> Skip(int n)
        {
            var q = Clone<T>();
            q._offset = n;
            return q;
        }

        public T ElementAt(int index)
        {
            return Skip(index).Take(1).First();
        }

        public TableQuery<T> Deferred()
        {
            var q = Clone<T>();
            q._deferred = true;
            return q;
        }

        public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        public TableQuery<T> ThenBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> ThenByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpr, bool asc)
        {
            if (orderExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)orderExpr;
                var unary = lambda.Body as UnaryExpression;

                MemberExpression mem;
                if (unary != null && unary.NodeType == ExpressionType.Convert)
                {
                    mem = unary.Operand as MemberExpression;
                }
                else
                {
                    mem = lambda.Body as MemberExpression;
                }

                if (mem != null && mem.Expression.NodeType == ExpressionType.Parameter)
                {
                    var q = Clone<T>();
                    if (q._orderBys == null)
                    {
                        q._orderBys = new List<Ordering>();
                    }
                    q._orderBys.Add(new Ordering
                    {
                        ColumnName = Table.FindColumnWithPropertyName(mem.Member.Name).Name,
                        Ascending = asc,
                    });
                    return q;
                }
                else
                {
                    throw new NotSupportedException(
                        "Order By does not support: " + orderExpr);
                }
            }
            else
            {
                throw new NotSupportedException("Must be a predicate");
            }
        }

        public List<T> ToList()
        {
            return GenerateCommand("*").ExecuteQuery<T>(Table);
        }

        public T[] ToArray()
        {
            return GenerateCommand("*").ExecuteQuery<T>(Table).ToArray();
        }

        void AddWhere(Expression pred)
        {
            if (_where == null)
            {
                _where = pred;
            }
            else
            {
                _where = Expression.AndAlso(_where, pred);
            }
        }

        private IDbCommand GenerateCommand(string selectionList)
        {
            if (Connection.IsSqlServer())
            {
                return GenerateCommandForSqlServer(selectionList);
            }
            else if (Connection.IsPostgreSql())
            {
                return GenerateCommandForPostgreSql(selectionList);
            }
            else
            {
                return GenerateCommandForSqlite(selectionList);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string EscapeLiteral(string literal) =>
            Connection.EscapeLiteral(literal);

        private IDbCommand GenerateCommandForPostgreSql(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            commandText.Append(selectionList);
            commandText.Append(" from ");
            commandText.Append(EscapeLiteral(Table.TableName));
            commandText.Append(" ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append(EscapeLiteral(_orderBys[0].ColumnName));
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", ");
                    commandText.Append(EscapeLiteral(_orderBys[i].ColumnName));
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_limit.HasValue)
            {
                commandText.Append(" limit ");
                commandText.Append(_limit.Value);
            }

            if (_offset.HasValue)
            {
                commandText.Append(" offset ");
                commandText.Append(_offset.Value);
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private IDbCommand GenerateCommandForSqlite(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            commandText.Append(selectionList);
            commandText.Append(" from [");
            commandText.Append(Table.TableName);
            commandText.Append("] ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append("[");
                commandText.Append(_orderBys[0].ColumnName);
                commandText.Append("]");
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", [");
                    commandText.Append(_orderBys[i].ColumnName);
                    commandText.Append("]");
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_limit.HasValue)
            {
                commandText.Append(" limit ");
                commandText.Append(_limit.Value);
            }

            if (_offset.HasValue)
            {
                if (!_limit.HasValue)
                {
                    commandText.Append(" limit -1 ");
                }
                commandText.Append(" offset ");
                commandText.Append(_offset.Value);
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private IDbCommand GenerateCommandForSqlServer(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            if (_limit.HasValue && !_offset.HasValue)
            {
                commandText.Append(" TOP (");
                commandText.Append(_limit.Value);
                commandText.Append(" ) ");
            }
            commandText.Append(selectionList);
            commandText.Append(" from [");
            commandText.Append(Table.TableName);
            commandText.Append("] ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append("[");
                commandText.Append(_orderBys[0].ColumnName);
                commandText.Append("]");
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", [");
                    commandText.Append(_orderBys[i].ColumnName);
                    commandText.Append("]");
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_offset.HasValue)
            {
                if (_orderBys == null || _orderBys.Count == 0)
                {
                    commandText.Append(" order by ");
                    commandText.Append("[");
                    commandText.Append(Table.PK.Name);
                    commandText.Append("] ");
                }

                commandText.Append(" OFFSET ");
                commandText.Append(_offset.Value);
                commandText.Append(" ROWS ");

                if (_limit.HasValue)
                {
                    commandText.Append(" FETCH NEXT ");
                    commandText.Append(_limit.Value);
                    commandText.Append(" ROWS ONLY ");
                }
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private readonly struct CompileResult
        {
            public CompileResult(string commandText, object value = null)
            {
                CommandText = commandText;
                Value = value;
            }

            public readonly string CommandText;

            public readonly object Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetParameterName(string name) =>
            Connection.GetParameterName(name);

        private CompileResult CompileExpr(Expression expr, List<object> queryArgs)
        {
            if (expr == null)
            {
                throw new NotSupportedException("Expression is NULL");
            }
            else if (expr is BinaryExpression)
            {
                var bin = (BinaryExpression)expr;
                var leftr = CompileExpr(bin.Left, queryArgs);
                var rightr = CompileExpr(bin.Right, queryArgs);

                string text;
                if (leftr.CommandText == GetParameterName("p" + queryArgs.Count.ToString()) && leftr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, rightr);
                }
                else if (rightr.CommandText == GetParameterName("p" + queryArgs.Count.ToString()) && rightr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, leftr);
                }
                else
                {
                    text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";
                }
                return new CompileResult(commandText: text);
            }
            else if (expr.NodeType == ExpressionType.Not)
            {
                var operandExpr = ((UnaryExpression)expr).Operand;
                var opr = CompileExpr(operandExpr, queryArgs);
                var val = opr.Value;
                if (val is bool)
                {
                    val = !((bool)val);
                }
                return new CompileResult(
                    commandText: "not(" + opr.CommandText + ")",
                    value: val);
            }
            else if (expr.NodeType == ExpressionType.Call)
            {
                var call = (MethodCallExpression)expr;
                var args = new CompileResult[call.Arguments.Count];

                CompileResult? obj;
                if (call.Object != null)
                {
                    obj = CompileExpr(call.Object, queryArgs);
                }
                else
                {
                    obj = null;
                }

                for (var i = 0; i < args.Length; i++)
                {
                    args[i] = CompileExpr(call.Arguments[i], queryArgs);
                }

                var sqlCall = "";
                if (call.Method.Name == "Like" && args.Length == 2)
                {
                    sqlCall = "(" + args[0].CommandText + " like " + args[1].CommandText + ")";
                }
                else if (call.Method.Name == nameof(Enumerable.Contains) && args.Length == 2)
                {
                    sqlCall = "(" + args[1].CommandText + " in " + args[0].CommandText + ")";
                }
                else if (call.Method.Name == nameof(Enumerable.Contains) && args.Length == 1)
                {
                    if (call.Object != null && call.Object.Type == typeof(string))
                    {
                        if (Connection.IsSqlServer())
                        {
                            sqlCall = "( CHARINDEX(" + args[0].CommandText + "," + obj.Value.CommandText + ") > 0)";
                        }
                        else if (Connection.IsPostgreSql())
                        {
                            sqlCall = "( strpos(" + obj.Value.CommandText + "," + args[0].CommandText + ") > 0)";
                        }
                        else
                        {
                            sqlCall = "( instr(" + obj.Value.CommandText + "," + args[0].CommandText + ") > 0)";
                        }
                    }
                    else
                    {
                        sqlCall = "(" + args[0].CommandText + " in " + obj.Value.CommandText + ")";
                    }
                }
                else if (call.Method.Name == nameof(string.StartsWith) && args.Length >= 1)
                {
                    var startsWithCmpOp = StringComparison.CurrentCulture;
                    if (args.Length == 2)
                    {
                        startsWithCmpOp = (StringComparison)args[1].Value;
                    }
                    switch (startsWithCmpOp)
                    {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "( SUBSTRING(" + obj.Value.CommandText + ", 1, " + args[0].Value.ToString().Length + ") = " + args[0].CommandText + ")";
                            }
                            else
                            {
                                sqlCall = "( substr(" + obj.Value.CommandText + ", 1, " + args[0].Value.ToString().Length + ") = " + args[0].CommandText + ")";
                            }
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like (" + args[0].CommandText + " + N'%'))";
                            }
                            else
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like (" + args[0].CommandText + " || '%'))";
                            }
                            break;
                    }
                }
                else if (call.Method.Name == nameof(string.EndsWith) && args.Length == 1)
                {
                    var endsWithCmpOp = StringComparison.CurrentCulture;
                    if (args.Length == 2)
                    {
                        endsWithCmpOp = (StringComparison)args[1].Value;
                    }
                    switch (endsWithCmpOp)
                    {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "( SUBSTRING("
                                    + obj.Value.CommandText
                                    + ", LEN("
                                    + obj.Value.CommandText
                                    + ") - "
                                    + args[0].Value.ToString().Length
                                    + "+1, "
                                    + args[0].Value.ToString().Length
                                    + ") = "
                                    + args[0].CommandText
                                    + ")";
                            }
                            else
                            {
                                sqlCall = "( substr("
                                    + obj.Value.CommandText
                                    + ", length("
                                    + obj.Value.CommandText
                                    + ") - "
                                    + args[0].Value.ToString().Length
                                    + "+1, "
                                    + args[0].Value.ToString().Length
                                    + ") = "
                                    + args[0].CommandText
                                    + ")";
                            }
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like (N'%' + " + args[0].CommandText + "))";
                            }
                            else
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like ('%' || " + args[0].CommandText + "))";
                            }
                            break;
                    }
                }
                else if (call.Method.Name == nameof(object.Equals) && args.Length == 1)
                {
                    sqlCall = "(" + obj.Value.CommandText + " = (" + args[0].CommandText + "))";
                }
                else if (call.Method.Name == nameof(string.ToLower))
                {
                    sqlCall = "(lower(" + obj.Value.CommandText + "))";
                }
                else if (call.Method.Name == nameof(string.ToUpper))
                {
                    sqlCall = "(upper(" + obj.Value.CommandText + "))";
                }
                else if (call.Method.Name == nameof(string.Replace) && args.Length == 2)
                {
                    sqlCall = "(replace(" + obj.Value.CommandText + "," + args[0].CommandText + "," + args[1].CommandText + "))";
                }
                else
                {
                    sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", args.Select(a => a.CommandText)) + ")";
                }
                return new CompileResult(
                    commandText: sqlCall);
            }
            else if (expr.NodeType == ExpressionType.Constant)
            {
                var c = (ConstantExpression)expr;
                queryArgs.Add(c.Value);
                return new CompileResult(
                    commandText: GetParameterName("p" + queryArgs.Count.ToString()),
                    value: c.Value);
            }
            else if (expr.NodeType == ExpressionType.Convert)
            {
                var u = (UnaryExpression)expr;
                var ty = u.Type;
                var valr = CompileExpr(u.Operand, queryArgs);
                return new CompileResult(
                    commandText: valr.CommandText,
                    value: valr.Value != null ? ConvertTo(valr.Value, ty) : null);
            }
            else if (expr.NodeType == ExpressionType.MemberAccess)
            {
                var mem = (MemberExpression)expr;

                var paramExpr = mem.Expression as ParameterExpression;
                if (paramExpr == null)
                {
                    var convert = mem.Expression as UnaryExpression;
                    if (convert != null && convert.NodeType == ExpressionType.Convert)
                    {
                        paramExpr = convert.Operand as ParameterExpression;
                    }
                }

                if (paramExpr != null)
                {
                    var columnName = Table.FindColumnWithPropertyName(mem.Member.Name).Name;
                    return new CompileResult(
                        commandText: EscapeLiteral(columnName));
                }
                else
                {
                    object obj = null;
                    if (mem.Expression != null)
                    {
                        var r = CompileExpr(mem.Expression, queryArgs);
                        if (r.Value == null)
                        {
                            throw new NotSupportedException(
                                "Member access failed to compile expression");
                        }
                        if (r.CommandText == GetParameterName("p" + queryArgs.Count.ToString()))
                        {
                            queryArgs.RemoveAt(queryArgs.Count - 1);
                        }
                        obj = r.Value;
                    }

                    // Get the member value
                    object val = null;
                    if (mem.Member is PropertyInfo)
                    {
                        var m = (PropertyInfo)mem.Member;
                        val = m.GetValue(obj, null);
                    }
                    else if (mem.Member is FieldInfo)
                    {
                        var m = (FieldInfo)mem.Member;
                        val = m.GetValue(obj);
                    }
                    else
                    {
                        throw new NotSupportedException(
                            "MemberExpr:" + mem.Member.GetType());
                    }

                    // Work special magic for enumerables
                    if (val != null && val is IEnumerable && !(val is string) && !(val is IEnumerable<byte>))
                    {
                        var sb = new StringBuilder();
                        sb.Append("(");
                        var head = "";
                        foreach (var a in (IEnumerable)val)
                        {
                            queryArgs.Add(a);
                            sb.Append(head);
                            sb.Append(GetParameterName("p" + queryArgs.Count.ToString()));
                            head = ",";
                        }
                        sb.Append(")");
                        return new CompileResult(
                            commandText: sb.ToString(),
                            value: val);
                    }
                    else
                    {
                        queryArgs.Add(val);
                        return new CompileResult(
                            commandText: GetParameterName("p" + queryArgs.Count.ToString()),
                            value: val);
                    }
                }
            }
            throw new NotSupportedException(
                $"Cannot compile: {expr.NodeType}");
        }

        private static object ConvertTo(object obj, Type t)
        {
            var nut = Nullable.GetUnderlyingType(t);
            if (nut != null)
            {
                if (obj == null)
                {
                    return null;
                }
                else
                {
                    return Convert.ChangeType(obj, nut);
                }
            }
            else
            {
                return Convert.ChangeType(obj, t);
            }
        }

        private string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    if (parameter.Value == null)
                        return "(" + parameter.CommandText + " is null)";
                    else
                        return "(" + parameter.CommandText + " is $" + parameter.CommandText.TrimStart('[').TrimEnd(']') + ")";
                case ExpressionType.NotEqual:
                    if (parameter.Value == null)
                        return "(" + parameter.CommandText + " is not null)";
                    else
                        return "(" + parameter.CommandText + " is not $" + parameter.CommandText.TrimStart('[').TrimEnd(']') + ")";
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    return "(" + parameter.CommandText + " < " + parameter.CommandText.TrimStart('[').TrimEnd(']') + ")";
                default:
                    throw new NotSupportedException(
                        $"Cannot compile Null-BinaryExpression with type {expression.NodeType}");
            }
        }

        private static string GetSqlName(Expression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.And:
                    return "&";
                case ExpressionType.AndAlso:
                    return "and";
                case ExpressionType.Or:
                    return "|";
                case ExpressionType.OrElse:
                    return "or";
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                default:
                    throw new NotSupportedException(
                        $"Cannot get SQL for: {expr.NodeType}");
            }
        }

        public int Count()
        {
            using (var command = GenerateCommand("count(*)"))
            {
                var result = command.ExecuteScalar();
                if (result is long l)
                {
                    return (int)l;
                }
                return (int)result;
            }
        }

        public int Count(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).Count();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ToList().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public T First()
        {
            var query = Take(1);
            return query.ToList().First();
        }

        public T First(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).First();
        }

        public T FirstOrDefault()
        {
            var query = Take(1);
            return query.ToList().FirstOrDefault();
        }

        public T FirstOrDefault(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).FirstOrDefault();
        }
    }

    static class FastSetter
    {
        internal static class Typeless
        {
            private static readonly MethodInfo _getSetterMethodInfo;

            static Typeless()
            {
                _getSetterMethodInfo = typeof(FastSetter).GetMethod(
                    nameof(FastSetter.GetFastSetter),
                    BindingFlags.NonPublic | BindingFlags.Static);
            }

            internal static Action<object, IDataRecord, int> GetFastSetter(
                TableMapping.Column column)
            {
                var getSetter = _getSetterMethodInfo.MakeGenericMethod(column.MappedType);
                return (Action<object, IDataRecord, int>)getSetter.Invoke(null, new object[] { column });
            }
        }

        internal static Action<object, IDataRecord, int> GetFastSetter<T>(
            TableMapping.Column column)
        {
            Action<object, IDataRecord, int> fastSetter = null;
            var clrType = column.PropertyInfo.PropertyType;
            var clrTypeInfo = clrType.GetTypeInfo();

            if (clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                clrType = clrTypeInfo.GenericTypeArguments[0];
                clrTypeInfo = clrType.GetTypeInfo();
            }

            if (clrType == typeof(string))
            {
                fastSetter = CreateTypedSetterDelegate<T, string>(column, (r, i) =>
                {
                    return r.GetString(i);
                });
            }
            else if (clrType == typeof(bool))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, bool>(column, (r, i) =>
                {
                    return r.GetBoolean(i);
                });
            }
            else if (clrType == typeof(int))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, int>(column, (r, i) =>
                {
                    return r.GetInt32(i);
                });
            }
            else if (clrType == typeof(long))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, long>(column, (r, i) =>
                {
                    return r.GetInt64(i);
                });
            }
            else if (clrType == typeof(short))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, short>(column, (r, i) =>
                {
                    return r.GetInt16(i);
                });
            }
            else if (clrType == typeof(decimal))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, decimal>(column, (r, i) =>
                {
                    return r.GetDecimal(i);
                });
            }
            else if (clrType == typeof(double))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, double>(column, (r, i) =>
                {
                    return r.GetDouble(i);
                });
            }
            else if (clrType == typeof(float))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, float>(column, (r, i) =>
                {
                    var value = r.GetValue(i);
                    if (value is double d)
                    {
                        return (float)d;
                    }
                    else
                    {
                        return (float)value;
                    }
                });
            }
            else if (clrType == typeof(DateTime))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, DateTime>(column, (r, i) =>
                {
                    return r.GetDateTime(i);
                });
            }
            else if (clrType == typeof(DateTimeOffset))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, DateTimeOffset>(column, (r, i) =>
                {
                    if (r is DbDataReader dbDataReader)
                    {
                        return dbDataReader.GetFieldValue<DateTimeOffset>(i);
                    }
                    var value = r.GetValue(i);
                    if (value is string s)
                    {
                        return DateTimeOffset.Parse(s);
                    }
                    else
                    {
                        return (DateTimeOffset)value;
                    }
                });
            }
            else if (clrType == typeof(TimeSpan))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, TimeSpan>(column, (r, i) =>
                {
                    var value = r.GetValue(i);
                    if (value is string s)
                    {
                        return TimeSpan.Parse(s);
                    }
                    else
                    {
                        return (TimeSpan)value;
                    }
                });
            }
            else if (clrType == typeof(byte[]))
            {
                fastSetter = CreateTypedSetterDelegate<T, byte[]>(column, (r, i) =>
                {
                    return (byte[])r.GetValue(i);
                });
            }
            else if (clrType == typeof(byte))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, byte>(column, (r, i) =>
                {
                    return r.GetByte(i);
                });
            }
            else if (clrType == typeof(sbyte))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, sbyte>(column, (r, i) =>
                {
                    return (sbyte)Convert.ChangeType(r.GetValue(i), typeof(sbyte));
                });
            }
            else if (clrType == typeof(uint))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, uint>(column, (r, i) =>
                {
                    return (uint)Convert.ChangeType(r.GetValue(i), typeof(uint));
                });
            }
            else if (clrType == typeof(ulong))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, ulong>(column, (r, i) =>
                {
                    return (ulong)Convert.ChangeType(r.GetValue(i), typeof(ulong));
                });
            }
            else if (clrType == typeof(ushort))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, ushort>(column, (r, i) =>
                {
                    return (ushort)Convert.ChangeType(r.GetValue(i), typeof(ushort));
                });
            }
            else if (clrType == typeof(Guid))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, Guid>(column, (r, i) =>
                {
                    return r.GetGuid(i);
                });
            }
            else if (clrType == typeof(Uri))
            {
                fastSetter = CreateTypedSetterDelegate<T, Uri>(column, (r, i) =>
                {
                    var s = r.GetString(i);
                    return new Uri(s);
                });
            }
            else if (clrType == typeof(StringBuilder))
            {
                fastSetter = CreateTypedSetterDelegate<T, StringBuilder>(column, (r, i) =>
                {
                    var s = r.GetString(i);
                    return new StringBuilder(s);
                });
            }
            else if (clrType == typeof(UriBuilder))
            {
                fastSetter = CreateTypedSetterDelegate<T, UriBuilder>(column, (r, i) =>
                {
                    var s = r.GetString(i);
                    return new UriBuilder(s);
                });
            }

            return fastSetter;
        }

        private static Action<object, IDataRecord, int> CreateNullableTypedSetterDelegate<TObject, TColumnMember>(
            TableMapping.Column column,
            Func<IDataRecord, int, TColumnMember> getColumnValue)
            where TColumnMember : struct
        {
            var clrTypeInfo = column.PropertyInfo.PropertyType.GetTypeInfo();
            var isNullable = clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>);
            if (isNullable)
            {
                var setProperty = (Action<TObject, TColumnMember?>)Delegate.CreateDelegate(
                    typeof(Action<TObject, TColumnMember?>),
                    null,
                    column.PropertyInfo.GetSetMethod());
                return (o, r, i) =>
                {
                    if (!r.IsDBNull(i))
                    {
                        setProperty.Invoke((TObject)o, getColumnValue(r, i));
                    }
                };
            }
            return CreateTypedSetterDelegate<TObject, TColumnMember>(column, getColumnValue);
        }

        private static Action<object, IDataRecord, int> CreateTypedSetterDelegate<TObject, TColumnMember>(
            TableMapping.Column column,
            Func<IDataRecord, int, TColumnMember> getColumnValue)
        {
            var setProperty = (Action<TObject, TColumnMember>)Delegate.CreateDelegate(
                typeof(Action<TObject, TColumnMember>),
                null,
                column.PropertyInfo.GetSetMethod());
            return (o, r, i) =>
            {
                if (!r.IsDBNull(i))
                {
                    setProperty.Invoke((TObject)o, getColumnValue(r, i));
                }
            };
        }
    }
}
