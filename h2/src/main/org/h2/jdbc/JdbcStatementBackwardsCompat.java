/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.SQLException;

/**
 * Allows us to compile on older platforms, while still implementing the methods
 * from the newer JDBC API.
 */
public interface JdbcStatementBackwardsCompat {

    // compatibility interface

    // JDBC 4.3 (incomplete)

    /**
     * Enquotes the specified identifier.
     *
     * @param identifier
     *            identifier to quote if required
     * @param alwaysQuote
     *            if {@code true} identifier will be quoted unconditionally
     * @return specified identifier quoted if required or explicitly requested
     * @throws SQLException on failure
     */
    String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException;

    /**
     * Checks if specified identifier may be used without quotes.
     *
     * @param identifier
     *            identifier to check
     * @return is specified identifier may be used without quotes
     * @throws SQLException on failure
     */
    boolean isSimpleIdentifier(String identifier) throws SQLException;
}
