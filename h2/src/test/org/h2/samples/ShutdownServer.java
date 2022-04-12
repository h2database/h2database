/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.SQLException;

/**
 * This very simple sample application stops a H2 TCP server
 * if it is running.
 */
public class ShutdownServer {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     * @throws SQLException on failure
     */
    public static void main(String... args) throws SQLException {
        org.h2.tools.Server.shutdownTcpServer("tcp://localhost:9094", "", false, false);
    }
}
