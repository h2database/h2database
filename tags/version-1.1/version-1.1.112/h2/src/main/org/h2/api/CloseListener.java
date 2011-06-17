/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.SQLException;

/**
 * A trigger that implements this interface will be notified when the database
 * is closed.
 */
public interface CloseListener {

    /**
     * This method is called when the database is closed.
     * If the method throws an exception, it will be logged, but
     * closing the database will continue.
     *
     * @throws SQLException
     */
    void close() throws SQLException;

    /**
     * This method is called when the trigger is dropped.
     *
     * @throws SQLException
     */
    void remove() throws SQLException;

}
