/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.function.CoalesceFunction;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * A wildcard expression as in SELECT * FROM TEST.
 * This object is only used temporarily during the parsing phase, and later
 * replaced by column expressions.
 */
public final class Wildcard extends Expression {

    private final String schema;
    private final String table;

    private ArrayList<ExpressionColumn> exceptColumns;

    public Wildcard(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public void setExceptColumns(ArrayList<ExpressionColumn> exceptColumns) {
        this.exceptColumns = exceptColumns;
    }

    @Override
    public Value getValue(SessionLocal session) {
        throw DbException.getInternalError(toString());
    }

    @Override
    public TypeInfo getType() {
        throw DbException.getInternalError(toString());
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        throw DbException.get(ErrorCode.SYNTAX_ERROR_1, table);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        throw DbException.get(ErrorCode.SYNTAX_ERROR_1, table);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        throw DbException.getInternalError(toString());
    }

    @Override
    public String getTableAlias() {
        return table;
    }

    @Override
    public String getSchemaName() {
        return schema;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (table != null) {
            StringUtils.quoteIdentifier(builder, table).append('.');
        }
        builder.append('*');
        if (exceptColumns != null) {
            writeExpressions(builder.append(" EXCEPT ("), exceptColumns, sqlFlags).append(')');
        }
        return builder;
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        throw DbException.getInternalError(toString());
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.QUERY_COMPARABLE) {
            return true;
        }
        throw DbException.getInternalError(Integer.toString(visitor.getType()));
    }

    @Override
    public int getCost() {
        throw DbException.getInternalError(toString());
    }

    public Collection<? extends Expression> expandExpressions(final SessionLocal session, final Collection<? extends ColumnResolver> resolvers) {
        final ArrayList<Expression> expressions = new ArrayList<>();
        int i = 0;
        HashMap<Column, ExpressionColumn> exceptTableColumns = null;
        if (table == null) {
            if (exceptColumns != null) {
                for (final ColumnResolver filter : resolvers) {
                    for (final ExpressionColumn column : exceptColumns) {
                        column.mapColumns(filter, 1, Expression.MAP_INITIAL);
                    }
                }
                exceptTableColumns = mapExceptColumns();
            }
            for (final ColumnResolver filter : resolvers) {
                if (filter instanceof TableFilter) {
                    i = expandColumnListForTableFilter(session, (TableFilter) filter, i, exceptTableColumns, expressions);
                } else {
                    i = expandColumnList(session, filter, i, exceptTableColumns, expressions);
                }
            }
        } else {
            Database db = session.getDatabase();
            ColumnResolver filter = null;
            for (final ColumnResolver f : resolvers) {
                if (db.equalsIdentifiers(table, f.getTableAlias())) {
                    if (schema == null || db.equalsIdentifiers(schema, f.getSchemaName())) {
                        if (exceptColumns != null) {
                            for (final ExpressionColumn column : exceptColumns) {
                                column.mapColumns(f, 1, Expression.MAP_INITIAL);
                            }
                            exceptTableColumns = mapExceptColumns();
                        }
                        filter = f;
                        break;
                    }
                }
            }
            if (filter == null) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, table);
            }
            expandColumnList(session, filter, i, exceptTableColumns, expressions);
        }
        return expressions;
    }

    /**
     * Returns map of excluded table columns to expression columns and validates
     * that all columns are resolved and not duplicated.
     *
     * @return map of excluded table columns to expression columns
     */
    private HashMap<Column, ExpressionColumn> mapExceptColumns() {
        HashMap<Column, ExpressionColumn> exceptTableColumns = new HashMap<>();
        for (ExpressionColumn ec : exceptColumns) {
            Column column = ec.getColumn();
            if (column == null) {
                throw ec.getColumnException(ErrorCode.COLUMN_NOT_FOUND_1);
            }
            if (exceptTableColumns.putIfAbsent(column, ec) != null) {
                throw ec.getColumnException(ErrorCode.DUPLICATE_COLUMN_NAME_1);
            }
        }
        return exceptTableColumns;
    }

    private int expandColumnList(SessionLocal session, ColumnResolver resolver, int index, HashMap<Column, ExpressionColumn> except, ArrayList<Expression> expressions) {
        for (final Column c : resolver.getColumns()) {
            index = addExpandedColumn(session, resolver, index, except, schema, resolver.getTableAlias(), c, expressions);
        }
        return index;
    }

    private int expandColumnListForTableFilter(SessionLocal session, TableFilter filter, int index,
                                               HashMap<Column, ExpressionColumn> except, ArrayList<Expression> expressions) {
        String schema = filter.getSchemaName();
        String alias = filter.getTableAlias();
        LinkedHashMap<Column, Column> commonJoinColumns = filter.getCommonJoinColumns();
        if (commonJoinColumns != null) {
            TableFilter replacementFilter = filter.getCommonJoinColumnsFilter();
            String replacementSchema = replacementFilter.getSchemaName();
            String replacementAlias = replacementFilter.getTableAlias();
            for (Map.Entry<Column, Column> entry : commonJoinColumns.entrySet()) {
                Column left = entry.getKey(), right = entry.getValue();
                if (!filter.isCommonJoinColumnToExclude(right)
                        && (except == null || except.remove(left) == null && except.remove(right) == null)) {
                    Database database = session.getDatabase();
                    Expression e;
                    if (left == right
                            || DataType.hasTotalOrdering(left.getType().getValueType())
                            && DataType.hasTotalOrdering(right.getType().getValueType())) {
                        e = new ExpressionColumn(database, replacementSchema, replacementAlias,
                                replacementFilter.getColumnName(right));
                    } else {
                        e = new Alias(new CoalesceFunction(CoalesceFunction.COALESCE,
                                new ExpressionColumn(database, schema, alias, filter.getColumnName(left)),
                                new ExpressionColumn(database, replacementSchema, replacementAlias,
                                        replacementFilter.getColumnName(right))), //
                                left.getName(), true);
                    }
                    expressions.add(index++, e);
                }
            }
        }
        for (Column c : filter.getColumns()) {
            if (commonJoinColumns == null || !commonJoinColumns.containsKey(c)) {
                if (!filter.isCommonJoinColumnToExclude(c)) {
                    index = addExpandedColumn(session, filter, index, except, schema, alias, c, expressions);
                }
            }
        }
        return index;
    }

    private int addExpandedColumn(SessionLocal session, ColumnResolver filter, int index, HashMap<Column, ExpressionColumn> except,
                                  String schema, String alias, Column c, final List<Expression> expressions) {
        if ((except == null || except.remove(c) == null) && c.getVisible()) {
            ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), schema, alias, filter.getColumnName(c));
            expressions.add(index++, ec);
        }
        return index;
    }
}
