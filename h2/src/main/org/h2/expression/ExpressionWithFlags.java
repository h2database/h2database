/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

/**
 * Expression with flags.
 */
public interface ExpressionWithFlags {

    /**
     * Set the flags for this expression.
     *
     * @param flags
     *            the flags to set
     */
    void setFlags(int flags);

    /**
     * Returns the flags.
     *
     * @return the flags
     */
    int getFlags();

}
