/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

public class ShutdownServer {
    public static void main(String[] args) throws Exception {
        org.h2.tools.Server.shutdownTcpServer("tcp://localhost:9094", "", false);
     }
}
