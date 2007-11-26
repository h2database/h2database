/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.sql.SQLException;

public interface Service {
    void init(String[] args) throws Exception;
    String getURL();
    void start() throws SQLException;
    void listen();
    void stop();
    boolean isRunning();
    boolean getAllowOthers();
    String getName();
    String getType();
}
