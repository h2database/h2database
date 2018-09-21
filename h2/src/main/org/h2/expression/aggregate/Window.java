/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Window clause.
 */
public final class Window {

    private ArrayList<Expression> partitionBy;

    private ArrayList<SelectOrderBy> orderBy;

    private WindowFrame frame;

    private String parent;

    /**
     * @param builder
     *            string builder
     * @param orderBy
     *            ORDER BY clause, or null
     */
    static void appendOrderBy(StringBuilder builder, ArrayList<SelectOrderBy> orderBy) {
        if (orderBy != null && !orderBy.isEmpty()) {
            if (builder.charAt(builder.length() - 1) != '(') {
                builder.append(' ');
            }
            builder.append("ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                SelectOrderBy o = orderBy.get(i);
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(o.expression.getSQL());
                SortOrder.typeToString(builder, o.sortType);
            }
        }
    }

    /**
     * Creates a new instance of window clause.
     *
     * @param parent
     *            name of the parent window
     * @param partitionBy
     *            PARTITION BY clause, or null
     * @param orderBy
     *            ORDER BY clause, or null
     * @param frame
     *            window frame clause
     */
    public Window(String parent, ArrayList<Expression> partitionBy, ArrayList<SelectOrderBy> orderBy,
            WindowFrame frame) {
        this.parent = parent;
        this.partitionBy = partitionBy;
        this.orderBy = orderBy;
        this.frame = frame;
    }

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver
     *            the column resolver
     * @param level
     *            the subquery nesting level
     * @see Expression#mapColumns(ColumnResolver, int)
     */
    public void mapColumns(ColumnResolver resolver, int level) {
        resolveWindows(resolver);
        if (partitionBy != null) {
            for (Expression e : partitionBy) {
                e.mapColumns(resolver, level);
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression.mapColumns(resolver, level);
            }
        }
    }

    private void resolveWindows(ColumnResolver resolver) {
        if (parent != null) {
            Window p = resolver.getSelect().getWindow(parent);
            if (p == null) {
                throw DbException.get(ErrorCode.WINDOW_NOT_FOUND_1, parent);
            }
            p.resolveWindows(resolver);
            if (partitionBy == null) {
                partitionBy = p.partitionBy;
            }
            if (orderBy == null) {
                orderBy = p.orderBy;
            }
            if (frame == null) {
                frame = p.frame;
            }
            parent = null;
        }
    }

    /**
     * Try to optimize the window conditions.
     *
     * @param session
     *            the session
     */
    public void optimize(Session session) {
        if (partitionBy != null) {
            for (int i = 0; i < partitionBy.size(); i++) {
                partitionBy.set(i, partitionBy.get(i).optimize(session));
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression = o.expression.optimize(session);
            }
        }
    }

    /**
     * Tell the expression columns whether the table filter can return values
     * now. This is used when optimizing the query.
     *
     * @param tableFilter
     *            the table filter
     * @param value
     *            true if the table filter can return value
     * @see Expression#setEvaluatable(TableFilter, boolean)
     */
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        if (partitionBy != null) {
            for (Expression e : partitionBy) {
                e.setEvaluatable(tableFilter, value);
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression.setEvaluatable(tableFilter, value);
            }
        }
    }

    /**
     * Returns ORDER BY clause.
     *
     * @return ORDER BY clause, or null
     */
    public ArrayList<SelectOrderBy> getOrderBy() {
        return orderBy;
    }

    /**
     * Returns window frame, or null.
     *
     * @return window frame, or null
     */
    public WindowFrame getWindowFrame() {
        return frame;
    }

    /**
     * Returns the key for the current group.
     *
     * @param session
     *            session
     * @return key for the current group, or null
     */
    public Value getCurrentKey(Session session) {
        if (partitionBy == null) {
            return null;
        }
        int len = partitionBy.size();
        if (len == 1) {
            return partitionBy.get(0).getValue(session);
        } else {
            Value[] keyValues = new Value[len];
            // update group
            for (int i = 0; i < len; i++) {
                Expression expr = partitionBy.get(i);
                keyValues[i] = expr.getValue(session);
            }
            return ValueArray.get(keyValues);
        }
    }

    /**
     * Returns SQL representation.
     *
     * @return SQL representation.
     * @see Expression#getSQL()
     */
    public String getSQL() {
        if (partitionBy == null && orderBy == null && frame == null) {
            return "OVER ()";
        }
        StringBuilder builder = new StringBuilder().append("OVER (");
        if (partitionBy != null) {
            builder.append("PARTITION BY ");
            for (int i = 0; i < partitionBy.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(StringUtils.unEnclose(partitionBy.get(i).getSQL()));
            }
        }
        appendOrderBy(builder, orderBy);
        if (frame != null && !frame.isDefault()) {
            if (builder.charAt(builder.length() - 1) != '(') {
                builder.append(' ');
            }
            builder.append(frame.getSQL());
        }
        return builder.append(')').toString();
    }

    /**
     * Update an aggregate value.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     * @see Expression#updateAggregate(Session, int)
     */
    public void updateAggregate(Session session, int stage) {
        if (partitionBy != null) {
            for (Expression expr : partitionBy) {
                expr.updateAggregate(session, stage);
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression.updateAggregate(session, stage);
            }
        }
    }

    @Override
    public String toString() {
        return getSQL();
    }

}
