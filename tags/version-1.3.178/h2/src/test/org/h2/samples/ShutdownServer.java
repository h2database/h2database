/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

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
     */
    public static void main(String... args) throws Exception {
        org.h2.tools.Server.shutdownTcpServer("tcp://localhost:9094", "", false, false);
    }
}
