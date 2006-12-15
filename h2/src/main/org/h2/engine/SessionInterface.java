/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.command.CommandInterface;
import org.h2.message.Trace;
import org.h2.store.DataHandler;

public interface SessionInterface {
    CommandInterface prepareCommand(String sql) throws SQLException;
    void close() throws SQLException;
    Trace getTrace();
    boolean isClosed();
    SessionInterface createSession(ConnectionInfo ci) throws SQLException;
    int getPowerOffCount();
    void setPowerOffCount(int i) throws SQLException;
    DataHandler getDataHandler();
}
