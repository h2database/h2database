/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.value.Value;

public interface ResultInterface {
    void reset() throws SQLException;
    Value[] currentRow();
    boolean next() throws SQLException;
    int getRowId();
    int getVisibleColumnCount();
    int getRowCount();
    void close();
    String getAlias(int i);
    String getSchemaName(int i);
    String getTableName(int i);
    String getColumnName(int i);
    int getColumnType(int i);
    long getColumnPrecision(int i);
    int getColumnScale(int i);
    int getDisplaySize(int i);
    boolean isAutoIncrement(int i);
    int getNullable(int i);
    boolean isUpdateCount();
    int getUpdateCount();
}
