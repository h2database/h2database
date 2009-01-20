/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.SQLException;

/**
 * This interface is for classes that create rows on demand.
 * It is used together with SimpleResultSet to create a dynamic result set.
 */
public interface SimpleRowSource {

    /**
     * Get the next row. Must return null if no more rows are available.
     *
     * @return the row or null
     * @throws SQLException
     */
    Object[] readRow() throws SQLException;

    /**
     * Close the row source.
     */
    void close();

    /**
     * Reset the position (before the first row).
     *
     * @throws SQLException if this operation is not supported
     */
    void reset() throws SQLException;
}
