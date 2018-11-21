/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import org.h2.expression.Expression;

/**
 * Window frame bound.
 */
public class WindowFrameBound {

    private final WindowFrameBoundType type;

    private final Expression value;

    /**
     * Creates new instance of window frame bound.
     *
     * @param type
     *            bound type
     * @param value
     *            bound value, if any
     */
    public WindowFrameBound(WindowFrameBoundType type, Expression value) {
        this.type = type;
        if (type == WindowFrameBoundType.PRECEDING || type == WindowFrameBoundType.FOLLOWING) {
            this.value = value;
        } else {
            this.value = null;
        }
    }

    /**
     * Returns the type
     *
     * @return the type
     */
    public WindowFrameBoundType getType() {
        return type;
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Appends SQL representation to the specified builder.
     *
     * @param builder
     *            string builder
     * @param following
     *            if false return SQL for starting clause, if true return SQL
     *            for following clause
     * @return the specified string builder
     * @see Expression#getSQL(StringBuilder)
     */
    public StringBuilder getSQL(StringBuilder builder, boolean following) {
        if (type == WindowFrameBoundType.PRECEDING || type == WindowFrameBoundType.FOLLOWING) {
            value.getSQL(builder).append(' ');
        }
        return builder.append(type.getSQL());
    }

}
