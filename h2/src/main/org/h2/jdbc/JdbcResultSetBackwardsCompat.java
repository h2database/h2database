/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.SQLException;

/**
 * Allows us to compile on older platforms, while still implementing the methods from the newer JDBC API.
 */
public interface JdbcResultSetBackwardsCompat {

    public abstract <T> T getObject(int columnIndex, Class<T> type) throws SQLException;

    public abstract <T> T getObject(String columnName, Class<T> type) throws SQLException;
}
