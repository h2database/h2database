package org.h2.test.jdbcx;

import java.sql.Connection;
import java.sql.Statement;

import org.h2.jdbcx.JdbcConnectionPoolManager;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;

public class TestConnectionPool extends TestBase {
    
    public void test() throws Exception {
        deleteDb("connectionPool");
        testConnect();
        testThreads();
    }
    
    private void testThreads() throws Exception {
        final int len = getSize(4, 20);
        final JdbcConnectionPoolManager man = getConnectionPool(len - 2);
        final boolean[] stop = new boolean[1];
        class TestRunner implements Runnable {
            public void run() {
                try {
                    while (!stop[0]) {
                        Connection conn = man.getConnection();
                        checkSmaller(man.getActiveConnections(), len + 1);
                        Statement stat = conn.createStatement();
                        stat.execute("SELECT 1 FROM DUAL");
                        conn.close();
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new Thread(new TestRunner());
            threads[i].start();
        }
        Thread.sleep(1000);
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        check(0, man.getActiveConnections());
        man.dispose();
    }
    
    JdbcConnectionPoolManager getConnectionPool(int poolSize) throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(getURL("connectionPool", true));
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        return new JdbcConnectionPoolManager(ds, poolSize, 2);
    }
    
    private void testConnect() throws Exception {
        JdbcConnectionPoolManager man = getConnectionPool(3);
        for (int i = 0; i < 100; i++) {
            Connection conn = man.getConnection();
            conn.close();
        }
        man.dispose();
    }

}
