/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.test.unit;

import org.h2.test.TestBase;
import org.h2.tools.Server;

public class TestFtp extends TestBase {

    public void test() throws Exception {
        test(baseDir);
    }

    private void test(String dir) throws Exception {
        Server server = Server.createFtpServer(new String[]{"-ftpDir", dir}).start();
        FtpClient client = FtpClient.open("localhost:8021");
        client.login("sa", "sa");
        client.makeDirectory("test");
        client.changeWorkingDirectory("test");
        client.makeDirectory("hello");
        client.changeWorkingDirectory("hello");
        client.changeDirectoryUp();
        client.nameList("hello");
        client.removeDirectory("hello");
        client.close();
        server.stop();
    }

}
