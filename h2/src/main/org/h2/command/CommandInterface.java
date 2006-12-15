/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.result.ResultInterface;
import org.h2.util.ObjectArray;

public interface CommandInterface {
    boolean isQuery();
    ObjectArray getParameters();
    ResultInterface executeQuery(int maxRows, boolean scrollable) throws SQLException;
    int executeUpdate() throws SQLException;
    void close();
    void cancel();
}
