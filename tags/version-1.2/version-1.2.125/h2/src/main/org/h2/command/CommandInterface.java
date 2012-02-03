/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;
import org.h2.util.ObjectArray;

/**
 * Represents a SQL statement.
 */
public interface CommandInterface {

    /**
     * Check if this is a query.
     *
     * @return true if it is a query
     */
    boolean isQuery();

    /**
     * Get the parameters (if any).
     *
     * @return the parameters
     */
    ObjectArray< ? extends ParameterInterface> getParameters();

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows returned
     * @param scrollable if the result set must be scrollable
     * @return the result
     */
    ResultInterface executeQuery(int maxRows, boolean scrollable) throws SQLException;

    /**
     * Execute the statement
     *
     * @return the update count
     */
    int executeUpdate() throws SQLException;

    /**
     * Close the statement.
     */
    void close();

    /**
     * Cancel the statement if it is still processing.
     */
    void cancel();

    /**
     * Get an empty result set containing the meta data of the result.
     *
     * @return the empty result
     */
    ResultInterface getMetaData() throws SQLException;
}
