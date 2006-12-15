/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.Statement;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;

public class TestDataSource extends TestBase {

    
//    public static void main(String[] args) throws Exception {
//        
//        // first, need to start on the command line:
//        // rmiregistry 1099
//        
//        // System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
//        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.rmi.registry.RegistryContextFactory");
//        System.setProperty(Context.PROVIDER_URL, "rmi://localhost:1099");
//        
//        JdbcDataSource ds = new JdbcDataSource();
//        ds.setURL("jdbc:h2:test");
//        ds.setUser("test");
//        ds.setPassword("");
//
//        Context ctx = new InitialContext();
//        ctx.bind("jdbc/test", ds);        
//        
//        DataSource ds2 = (DataSource)ctx.lookup("jdbc/test");
//        Connection conn = ds2.getConnection();
//        conn.close();
//    } 
    
    public void test() throws Exception {
        deleteDb("datasource");

        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:"+BASE_DIR+"/datasource");
        ds.setUser("sa");
        ds.setPassword("");
        Connection conn = ds.getConnection();
        Statement stat = conn.createStatement();
        stat.execute("SELECT * FROM DUAL");
        conn.close();
    }

}
