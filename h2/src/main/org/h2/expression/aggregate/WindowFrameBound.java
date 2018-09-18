/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.expression.Expression;
import org.h2.message.DbException;

/**
 * Window frame bound.
 */
public class WindowFrameBound {

    /**
     * Window frame bound type.
     */
    public enum WindowFrameBoundType {

    /**
     * UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING clause.
     */
    UNBOUNDED,

    /**
     * CURRENT_ROW clause.
     */
    CURRENT_ROW,

    /**
     * PRECEDING or FOLLOWING clause.
     */
    VALUE;

    }

    private final WindowFrameBoundType type;

    private final int value;

    /**
     * Creates new instance of window frame bound.
     *
     * @param type
     *            bound type
     * @param value
     *            bound value, if any
     */
    public WindowFrameBound(WindowFrameBoundType type, int value) {
        this.type = type;
        if (type == WindowFrameBoundType.VALUE) {
            if (value < 0) {
                throw DbException.getInvalidValueException("unsigned", value);
            }
            this.value = value;
        } else {
            this.value = 0;
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
    public int getValue() {
        return value;
    }

    /**
     * Returns SQL representation.
     *
     * @param following
     *            if false return SQL for starting clause, if true return SQL
     *            for following clause
     * @return SQL representation.
     * @see Expression#getSQL()
     */
    public String getSQL(boolean following) {
        switch (type) {
        case UNBOUNDED:
            return following ? "UNBOUNDED FOLLOWING" : "UNBOUNDED PRECEDING";
        case CURRENT_ROW:
            return "CURRENT ROW";
        case VALUE:
            return value + (following ? " FOLLOWING" : " PRECEDING");
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

}
