/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

import org.h2.api.ErrorCode;
import org.h2.command.query.QueryOrderBy;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.HasSQL;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Window clause.
 */
public final class Window {

    private ArrayList<Expression> partitionBy;

    private ArrayList<QueryOrderBy> orderBy;

    private WindowFrame frame;

    private String parent;

    /**
     * Appends ORDER BY clause to the specified builder.
     *
     * @param builder
     *            string builder
     * @param orderBy
     *            ORDER BY clause, or null
     * @param sqlFlags
     *            formatting flags
     * @param forceOrderBy
     *            whether synthetic ORDER BY clause should be generated when it
     *            is missing
     */
    public static void appendOrderBy(StringBuilder builder, ArrayList<QueryOrderBy> orderBy, int sqlFlags,
            boolean forceOrderBy) {
        if (orderBy != null && !orderBy.isEmpty()) {
            appendOrderByStart(builder);
            for (int i = 0; i < orderBy.size(); i++) {
                QueryOrderBy o = orderBy.get(i);
                if (i > 0) {
                    builder.append(", ");
                }
                o.expression.getUnenclosedSQL(builder, sqlFlags);
                SortOrder.typeToString(builder, o.sortType);
            }
        } else if (forceOrderBy) {
            appendOrderByStart(builder);
            builder.append("NULL");
        }
    }

    private static void appendOrderByStart(StringBuilder builder) {
        if (builder.charAt(builder.length() - 1) != '(') {
            builder.append(' ');
        }
        builder.append("ORDER BY ");
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
     *            window frame clause, or null
     */
    public Window(String parent, ArrayList<Expression> partitionBy, ArrayList<QueryOrderBy> orderBy,
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
     * @see Expression#mapColumns(ColumnResolver, int, int)
     */
    public void mapColumns(ColumnResolver resolver, int level) {
        resolveWindows(resolver);
        if (partitionBy != null) {
            for (Expression e : partitionBy) {
                e.mapColumns(resolver, level, Expression.MAP_IN_WINDOW);
            }
        }
        if (orderBy != null) {
            for (QueryOrderBy o : orderBy) {
                o.expression.mapColumns(resolver, level, Expression.MAP_IN_WINDOW);
            }
        }
        if (frame != null) {
            frame.mapColumns(resolver, level, Expression.MAP_IN_WINDOW);
        }
    }

    private void resolveWindows(ColumnResolver resolver) {
        if (parent != null) {
            Select select = resolver.getSelect();
            Window p;
            while ((p = select.getWindow(parent)) == null) {
                select = select.getParentSelect();
                if (select == null) {
                    throw DbException.get(ErrorCode.WINDOW_NOT_FOUND_1, parent);
                }
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
    public void optimize(SessionLocal session) {
        if (partitionBy != null) {
            for (ListIterator<Expression> i = partitionBy.listIterator(); i.hasNext();) {
                Expression e = i.next().optimize(session);
                if (e.isConstant()) {
                    i.remove();
                } else {
                    i.set(e);
                }
            }
            if (partitionBy.isEmpty()) {
                partitionBy = null;
            }
        }
        if (orderBy != null) {
            for (Iterator<QueryOrderBy> i = orderBy.iterator(); i.hasNext();) {
                QueryOrderBy o = i.next();
                Expression e = o.expression.optimize(session);
                if (e.isConstant()) {
                    i.remove();
                } else {
                    o.expression = e;
                }
            }
            if (orderBy.isEmpty()) {
                orderBy = null;
            }
        }
        if (frame != null) {
            frame.optimize(session);
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
            for (QueryOrderBy o : orderBy) {
                o.expression.setEvaluatable(tableFilter, value);
            }
        }
    }

    /**
     * Returns ORDER BY clause.
     *
     * @return ORDER BY clause, or null
     */
    public ArrayList<QueryOrderBy> getOrderBy() {
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
     * Returns {@code true} if window ordering clause is specified or ROWS unit
     * is used.
     *
     * @return {@code true} if window ordering clause is specified or ROWS unit
     *         is used
     */
    public boolean isOrdered() {
        if (orderBy != null) {
            return true;
        }
        if (frame != null && frame.getUnits() == WindowFrameUnits.ROWS) {
            if (frame.getStarting().getType() == WindowFrameBoundType.UNBOUNDED_PRECEDING) {
                WindowFrameBound following = frame.getFollowing();
                if (following != null && following.getType() == WindowFrameBoundType.UNBOUNDED_FOLLOWING) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the key for the current group.
     *
     * @param session
     *            session
     * @return key for the current group, or null
     */
    public Value getCurrentKey(SessionLocal session) {
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
            return ValueRow.get(keyValues);
        }
    }

    /**
     * Appends SQL representation to the specified builder.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @param forceOrderBy
     *            whether synthetic ORDER BY clause should be generated when it
     *            is missing
     * @return the specified string builder
     * @see Expression#getSQL(StringBuilder, int, int)
     */
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags, boolean forceOrderBy) {
        builder.append("OVER (");
        if (partitionBy != null) {
            builder.append("PARTITION BY ");
            for (int i = 0; i < partitionBy.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                partitionBy.get(i).getUnenclosedSQL(builder, sqlFlags);
            }
        }
        appendOrderBy(builder, orderBy, sqlFlags, forceOrderBy);
        if (frame != null) {
            if (builder.charAt(builder.length() - 1) != '(') {
                builder.append(' ');
            }
            frame.getSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

    /**
     * Update an aggregate value.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     * @see Expression#updateAggregate(SessionLocal, int)
     */
    public void updateAggregate(SessionLocal session, int stage) {
        if (partitionBy != null) {
            for (Expression expr : partitionBy) {
                expr.updateAggregate(session, stage);
            }
        }
        if (orderBy != null) {
            for (QueryOrderBy o : orderBy) {
                o.expression.updateAggregate(session, stage);
            }
        }
        if (frame != null) {
            frame.updateAggregate(session, stage);
        }
    }

    @Override
    public String toString() {
        return getSQL(new StringBuilder(), HasSQL.TRACE_SQL_FLAGS, false).toString();
    }

}
