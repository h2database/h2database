/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;

/**
 * Tests DataSource and XAConnection.
 */
public class TestDataSource extends TestBase {

//     public static void main(String[] args) throws SQLException {
//
//     // first, need to start on the command line:
//     // rmiregistry 1099
//
//     // System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
//     "com.sun.jndi.ldap.LdapCtxFactory");
//     System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
//     "com.sun.jndi.rmi.registry.RegistryContextFactory");
//     System.setProperty(Context.PROVIDER_URL, "rmi://localhost:1099");
//
//     JdbcDataSource ds = new JdbcDataSource();
//     ds.setURL("jdbc:h2:test");
//     ds.setUser("test");
//     ds.setPassword("");
//
//     Context ctx = new InitialContext();
//     ctx.bind("jdbc/test", ds);
//
//     DataSource ds2 = (DataSource)ctx.lookup("jdbc/test");
//     Connection conn = ds2.getConnection();
//     conn.close();
//     }

    public void test() throws Exception {
        testDataSource();
        testXAConnection();
        deleteDb(baseDir, "dataSource");
    }

    private void testXAConnection() throws Exception {
        deleteDb(baseDir, "dataSource");
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:" + baseDir + "/dataSource");
        XAConnection xaConn = ds.getXAConnection();
        xaConn.addConnectionEventListener(new ConnectionEventListener() {
            public void connectionClosed(ConnectionEvent event) {
                // nothing to do
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
                // nothing to do
            }
        });
        XAResource res = xaConn.getXAResource();
        Connection conn = xaConn.getConnection();
        Xid[] list = res.recover(XAResource.TMSTARTRSCAN);
        assertEquals(list.length, 0);
        Statement stat = conn.createStatement();
        stat.execute("SELECT * FROM DUAL");
        conn.close();
        xaConn.close();
    }

    private void testDataSource() throws SQLException {
        deleteDb(baseDir, "dataSource");
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:" + baseDir + "/dataSource");
        ds.setUser("sa");
        Connection conn = ds.getConnection();
        Statement stat = conn.createStatement();
        stat.execute("SELECT * FROM DUAL");
        conn.close();
    }

}
