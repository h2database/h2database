/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

/**
 * Window frame units.
 */
public enum WindowFrameUnits {

    /**
     * ROWS unit.
     */
    ROWS,

    /**
     * RANGE unit.
     */
    RANGE,

    /**
     * GROUPS unit.
     */
    GROUPS,

    ;

    /**
     * Returns SQL representation.
     *
     * @return SQL representation.
     * @see org.h2.expression.Expression#getSQL(int)
     */
    public String getSQL() {
        return name();
    }

}
