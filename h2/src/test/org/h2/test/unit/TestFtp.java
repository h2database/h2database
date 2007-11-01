package org.h2.test.unit;

import java.sql.SQLException;

import org.h2.test.TestBase;
import org.h2.tools.Server;

public class TestFtp extends TestBase {

    public void test() throws Exception {
        test(baseDir);
    }

    private void test(String dir) throws SQLException {
        Server server = Server.createFtpServer(new String[]{"-ftpDir", dir}).start();
        
        server.stop();
    }

}
