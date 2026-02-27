using System;
using System.Collections.Generic;
using System.Text;

namespace Kuery.Linq
{
    internal sealed class SelectSqlGenerator
    {
        readonly SqlPredicateTranslator _predicateTranslator = new SqlPredicateTranslator();

        internal GeneratedSql Generate(SelectQueryModel model, ISqlDialect dialect)
        {
            if (model == null) throw new ArgumentNullException(nameof(model));
            if (dialect == null) throw new ArgumentNullException(nameof(dialect));

            var parameters = new List<object>();
            var sql = new StringBuilder();

            var effectiveTake = model.Take;
            if ((model.Terminal == QueryTerminalKind.First || model.Terminal == QueryTerminalKind.FirstOrDefault) && !effectiveTake.HasValue)
            {
                effectiveTake = 1;
            }

            sql.Append("select ");
            if (model.Terminal == QueryTerminalKind.Count)
            {
                sql.Append("count(*)");
            }
            else
            {
                if (dialect.Kind == SqlDialectKind.SqlServer && effectiveTake.HasValue && !model.Skip.HasValue)
                {
                    sql.Append("TOP (");
                    sql.Append(effectiveTake.Value);
                    sql.Append(") ");
                }

                if (model.ProjectedColumns != null && model.ProjectedColumns.Count > 0)
                {
                    for (var i = 0; i < model.ProjectedColumns.Count; i++)
                    {
                        if (i > 0)
                        {
                            sql.Append(", ");
                        }
                        sql.Append(dialect.EscapeIdentifier(model.ProjectedColumns[i].SourceColumn.Name));
                    }
                }
                else
                {
                    sql.Append("*");
                }
            }

            sql.Append(" from ");
            sql.Append(dialect.EscapeIdentifier(model.Table.TableName));

            if (model.Predicate != null)
            {
                sql.Append(" where ");
                sql.Append(_predicateTranslator.Translate(model.Predicate, model.Table, dialect, parameters));
            }

            AppendOrderBy(sql, model, dialect);
            AppendPaging(sql, model, effectiveTake, dialect);

            return new GeneratedSql(sql.ToString(), parameters);
        }

        private static void AppendOrderBy(StringBuilder sql, SelectQueryModel model, ISqlDialect dialect)
        {
            if (model.Orderings.Count == 0)
            {
                if (model.Skip.HasValue && model.Table.PK != null)
                {
                    sql.Append(" order by ");
                    sql.Append(dialect.EscapeIdentifier(model.Table.PK.Name));
                }
                return;
            }

            sql.Append(" order by ");
            for (var i = 0; i < model.Orderings.Count; i++)
            {
                if (i > 0)
                {
                    sql.Append(", ");
                }

                var ordering = model.Orderings[i];
                sql.Append(dialect.EscapeIdentifier(ordering.Column.Name));
                if (!ordering.Ascending)
                {
                    sql.Append(" desc");
                }
            }
        }

        private static void AppendPaging(StringBuilder sql, SelectQueryModel model, int? effectiveTake, ISqlDialect dialect)
        {
            if (dialect.Kind == SqlDialectKind.SqlServer)
            {
                if (model.Skip.HasValue)
                {
                    sql.Append(" OFFSET ");
                    sql.Append(model.Skip.Value);
                    sql.Append(" ROWS");
                    if (effectiveTake.HasValue)
                    {
                        sql.Append(" FETCH NEXT ");
                        sql.Append(effectiveTake.Value);
                        sql.Append(" ROWS ONLY");
                    }
                }

                return;
            }

            if (effectiveTake.HasValue)
            {
                sql.Append(" limit ");
                sql.Append(effectiveTake.Value);
            }

            if (model.Skip.HasValue)
            {
                if (!effectiveTake.HasValue)
                {
                    if (dialect.Kind == SqlDialectKind.Sqlite)
                    {
                        sql.Append(" limit -1");
                    }
                    else
                    {
                        sql.Append(" limit 9223372036854775807");
                    }
                }

                sql.Append(" offset ");
                sql.Append(model.Skip.Value);
            }
        }
    }
}
