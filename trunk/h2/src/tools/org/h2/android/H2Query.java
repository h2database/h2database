/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import org.h2.command.Prepared;

/**
 * This class represents a prepared statement that returns a result set.
 */
public class H2Query extends H2Program {

    H2Query(Prepared prepared) {
        super(prepared);
    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindDouble(int index, double value) {
        // TODO
    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindLong(int index, long value) {
        // TODO
    }

    /**
     * Set the specified parameter to NULL.
     *
     * @param index the parameter index (0, 1,...)
     */
    public void bindNull(int index) {
        // TODO
    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindString(int index, String value) {
        // TODO
    }

    /**
     * Close the statement.
     */
    public void close() {
        // TODO
    }

}
