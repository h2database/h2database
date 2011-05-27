/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * This interface is used to extend the LocalResult class, if data does not fit
 * in memory.
 */
public interface ResultExternal {

    void reset() throws SQLException;

    Value[] next() throws SQLException;

    void addRows(ObjectArray rows) throws SQLException;

    void done() throws SQLException;

    void close();

    int removeRow(Value[] values) throws SQLException;

    boolean contains(Value[] values) throws SQLException;

    int addRow(Value[] values) throws SQLException;

}
