using System;
using System.Collections;
using System.Data;

namespace Kuery.Query.Internal
{
    internal sealed class CollectionResultCoordinator
    {
        readonly CollectionQueryPlan _plan;
        long _identifier;
        object[] _values;
        IList _elements;

        internal CollectionResultCoordinator(CollectionQueryPlan plan)
        {
            _plan = plan;
        }

        // Read the next group's first row before yielding the previous group.
        internal bool Read(IDataRecord record, out object completed)
        {
            var identifier = _plan.ReadIdentifier(record);
            var hasCompleted = _elements != null && identifier != _identifier;
            completed = hasCompleted ? Finish() : null;
            if (_elements == null || hasCompleted)
            {
                _identifier = identifier;
                _values = _plan.ReadOuterValues(record);
                _elements = (IList)Activator.CreateInstance(_plan.ListType);
            }

            if (_plan.HasElement(record))
                _elements.Add(_plan.ReadElement(record));

            return hasCompleted;
        }

        internal bool HasGroup => _elements != null;
        internal object Finish() => _plan.CreateResult(_values, _elements);
    }

    internal sealed class CollectionQueryPlan
    {
        internal Func<IDataRecord, long> ReadIdentifier;
        internal Func<IDataRecord, bool> HasElement;
        internal Func<IDataRecord, object> ReadElement;
        internal Func<IDataRecord, object[]> ReadOuterValues;
        internal Func<object[], IList, object> CreateResult;
        internal Type ListType;
    }
}
