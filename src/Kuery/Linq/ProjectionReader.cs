using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Kuery.Linq
{
    internal class ProjectionReader<T> : IEnumerable<T>
    {
        private Enumerator enumerator;

        internal ProjectionReader(DbDataReader reader, Func<ProjectionRow, T> projector)
        {

        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        class Enumerator : ProjectionRow, IEnumerator<T>
        {
            private DbDataReader reader;
            private T current;
            private Func<ProjectionRow, T> projector;

            internal Enumerator(DbDataReader reader, Func<ProjectionRow, T> projector)
            {
                this.reader = reader;
                this.projector = projector;
            }

            /// <inheritdoc/>
            internal override object GetValue(int index)
            {
                if (index >= 0)
                {
                    if (reader.IsDBNull(index))
                    {
                        return null;
                    }
                    else
                    {
                        return reader.GetValue(index);
                    }
                }
                throw new IndexOutOfRangeException();
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
                    current = projector(this);
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
        }
    }
}
