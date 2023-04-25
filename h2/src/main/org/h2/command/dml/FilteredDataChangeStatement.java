/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * Data change statement with WHERE criteria and possibly limited number of
 * rows.
 */
abstract class FilteredDataChangeStatement extends DataChangeStatement {

    /**
     * The WHERE criteria.
     */
    Expression condition;

    /**
     * The target table filter.
     */
    TableFilter targetTableFilter;

    /**
     * The expression with optional maximum number of rows.
     */
    Expression fetchExpr;

    /**
     * Creates new instance of FilteredDataChangeStatement.
     *
     * @param session
     *            the session
     */
    FilteredDataChangeStatement(SessionLocal session) {
        super(session);
    }

    @Override
    public final Table getTable() {
        return targetTableFilter.getTable();
    }

    public final void setTableFilter(TableFilter tableFilter) {
        this.targetTableFilter = tableFilter;
    }

    public final TableFilter getTableFilter() {
        return targetTableFilter;
    }

    public final void setCondition(Expression condition) {
        this.condition = condition;
    }

    public final Expression getCondition() {
        return this.condition;
    }

    public void setFetch(Expression fetch) {
        this.fetchExpr = fetch;
    }

    final boolean nextRow(long limitRows, long count) {
        if (limitRows < 0 || count < limitRows) {
            while (targetTableFilter.next()) {
                setCurrentRowNumber(count + 1);
                if (condition == null || condition.getBooleanValue(session)) {
                    return true;
                }
            }
        }
        return false;
    }

    final void appendFilterCondition(StringBuilder builder, int sqlFlags) {
        if (condition != null) {
            builder.append("\nWHERE ");
            condition.getUnenclosedSQL(builder, sqlFlags);
        }
        if (fetchExpr != null) {
            builder.append("\nFETCH FIRST ");
            String count = fetchExpr.getSQL(sqlFlags, Expression.WITHOUT_PARENTHESES);
            if ("1".equals(count)) {
                builder.append("ROW ONLY");
            } else {
                builder.append(count).append(" ROWS ONLY");
            }
        }
    }

}
