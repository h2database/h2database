/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.server.ftp.FtpEvent;
import org.h2.server.ftp.FtpEventListener;
import org.h2.server.ftp.FtpServer;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the FTP server tool.
 */
public class TestFtp extends TestBase implements FtpEventListener {

    private FtpEvent lastEvent;

    public void test() throws Exception {
        test(baseDir);
    }

    private void test(String dir) throws Exception {
        Server server = Server.createFtpServer(new String[]{"-ftpDir", dir, "-ftpPort", "8121"}).start();
        FtpServer ftp = (FtpServer) server.getService();
        ftp.setEventListener(this);
        FtpClient client = FtpClient.open("localhost:8121");
        client.login("sa", "sa");
        client.makeDirectory("test");
        client.changeWorkingDirectory("test");
        assertEquals(lastEvent.getCommand(), "CWD");
        client.makeDirectory("hello");
        client.changeWorkingDirectory("hello");
        client.changeDirectoryUp();
        assertEquals(lastEvent.getCommand(), "CDUP");
        client.nameList("hello");
        client.removeDirectory("hello");
        client.close();
        server.stop();
    }

    public void beforeCommand(FtpEvent event) {
        lastEvent = event;
    }

    public void afterCommand(FtpEvent event) {
        lastEvent = event;
    }

    public void onUnsupportedCommand(FtpEvent event) {
        lastEvent = event;
    }

}
