/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;

import org.h2.jdbcx.JdbcDataSource;

/**
 * A simple XA test.
 */
public class TestXASimple {

    private int notYetImplemented;

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");

        // InitialContext context = new InitialContext();
        // context.rebind(USER_TRANSACTION_JNDI_NAME, j.getUserTransaction());

        JdbcDataSource ds1 = new JdbcDataSource();
        ds1.setPassword("");
        ds1.setUser("sa");
        ds1.setURL("jdbc:h2:db1H2");

        JdbcDataSource ds2 = new JdbcDataSource();
        ds2.setPassword("");
        ds2.setUser("sa");
        ds2.setURL("jdbc:h2:db2H2");

        // UserTransaction ut = (UserTransaction)
        // context.lookup("UserTransaction");
        // ut.begin();

        Connection c1 = ds1.getXAConnection().getConnection();
        c1.setAutoCommit(false);
        Connection c2 = ds2.getXAConnection().getConnection();
        c2.setAutoCommit(false);

        c1.createStatement().executeUpdate("create table test(id int, test varchar(255))");
        c2.createStatement().executeUpdate("create table test(id int, test varchar(255))");

        // ut.rollback();
        c1.close();
        c2.close();

        // j.stop();
        // System.exit(0);

    }
}
