using System;
using System.Collections;
using System.Data;

namespace Kuery.Query.Internal
{
    /// <summary>Accumulates adjacent rows into a grouped result during enumeration.</summary>
    internal sealed class CollectionResultCoordinator
    {
        readonly CollectionQueryPlan _plan;
        long _identifier;
        object[] _values;
        IList _elements;

        /// <summary>Starts an empty coordinator with the compiled collection plan.</summary>
        /// <param name="plan">The compiled plan that supplies SQL or row materialization delegates.</param>
        internal CollectionResultCoordinator(CollectionQueryPlan plan)
        {
            _plan = plan;
        }

        // Read the next group's first row before yielding the previous group.
        /// <summary>Adds a row to its group and completes the previous group when the identifier changes.</summary>
        /// <param name="record">The current result row.</param>
        /// <param name="completed">The preceding group's result if a group was completed, otherwise null.</param>
        /// <returns>True if the row completed the preceding group.</returns>
        /// <remarks>Call for each row in grouping identifier order. After the final row, call Finish if a group is active.</remarks>
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

        /// <summary>Gets whether the coordinator has an active group.</summary>
        internal bool HasGroup => _elements != null;
        /// <summary>Constructs the result for the active group after all of its rows are read.</summary>
        /// <returns>The materialized result for the active group.</returns>
        /// <remarks>Call only after at least one row has initialized an active group.</remarks>
        internal object Finish() => _plan.CreateResult(_values, _elements);
    }

    /// <summary>Holds the delegates that read SQL rows and build a collection result.</summary>
    internal sealed class CollectionQueryPlan
    {
        /// <summary>The delegate that reads the SQL grouping identifier from a row.</summary>
        internal Func<IDataRecord, long> ReadIdentifier;
        /// <summary>The delegate that tests whether a row contains a collection element.</summary>
        internal Func<IDataRecord, bool> HasElement;
        /// <summary>The delegate that materializes a collection element.</summary>
        internal Func<IDataRecord, object> ReadElement;
        /// <summary>The delegate that reads the values shared by the group from its first row.</summary>
        internal Func<IDataRecord, object[]> ReadOuterValues;
        /// <summary>The delegate that combines the group's shared values with its accumulated elements.</summary>
        internal Func<object[], IList, object> CreateResult;
        /// <summary>The CLR list type used to accumulate collection elements.</summary>
        internal Type ListType;
    }
}
