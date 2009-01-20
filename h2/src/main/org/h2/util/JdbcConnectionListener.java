/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

import org.h2.jdbc.JdbcConnection;

/**
 * The JDBC connection listener is used internally by the H2 XA connection.
 */
public interface JdbcConnectionListener {

    // TODO pooled connection: make sure
    // fatalErrorOccurred is called in the right situations
    /**
     * A fatal error occurred.
     *
     * @param conn the connection
     * @param e the exception
     */
    void fatalErrorOccurred(JdbcConnection conn, SQLException e) throws SQLException;

    /**
     * A connection was closed
     *
     * @param conn the connection
     */
    void closed(JdbcConnection conn);
}
