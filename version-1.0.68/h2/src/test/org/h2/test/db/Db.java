/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;

/**
 * A simple wrapper around the JDBC API.
 * Currently used for testing.
 * Features:
 * <ul>
 * <li>No checked exceptions
 * </li><li>Easy to use, fluent API
 * </li></ul>
 */
public class Db {

    private static final String[] DRIVERS = {
        "jdbc:h2:", "org.h2.Driver",
        "jdbc:firebirdsql:", "org.firebirdsql.jdbc.FBDriver",
        "jdbc:db2:", "COM.ibm.db2.jdbc.net.DB2Driver",
        "jdbc:oracle:", "oracle.jdbc.driver.OracleDriver",
        "jdbc:microsoft:", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jdbc:postgresql:", "org.postgresql.Driver",
        "jdbc:mysql:", "com.mysql.jdbc.Driver",
        "jdbc:derby://", "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby:", "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:hsqldb:", "org.hsqldb.jdbcDriver"
    };

    private Connection conn;
    private Statement stat;
    private HashMap prepared = new HashMap();
    private long start;

    public static Db open(String url, String user, String password) {
        try {
            for (int i = 0; i < DRIVERS.length; i += 2) {
                String prefix = DRIVERS[i];
                if (url.startsWith(prefix)) {
                    Class.forName(DRIVERS[i + 1]);
                    break;
                }
            }
            return new Db(DriverManager.getConnection(url, user, password));
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public Prepared prepare(String sql) {
        try {
            PreparedStatement prep = (PreparedStatement) prepared.get(sql);
            if (prep == null) {
                prep = conn.prepareStatement(sql);
                prepared.put(sql, prep);
            }
            return new Prepared(conn.prepareStatement(sql));
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public void execute(String sql) {
        try {
            stat.execute(sql);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private Db(Connection conn) {
        try {
            this.conn = conn;
            stat = conn.createStatement();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public class Prepared {
        private PreparedStatement prep;
        private int index;

        Prepared(PreparedStatement prep) {
            this.prep = prep;
        }

        public Prepared set(int x) {
            try {
                prep.setInt(++index, x);
                return this;
            } catch (Exception e) {
                throw convert(e);
            }
        }

        public Prepared set(String x) {
            try {
                prep.setString(++index, x);
                return this;
            } catch (Exception e) {
                throw convert(e);
            }
        }

        public Prepared set(byte[] x) {
            try {
                prep.setBytes(++index, x);
                return this;
            } catch (Exception e) {
                throw convert(e);
            }
        }

        public Prepared set(InputStream x) {
            try {
                prep.setBinaryStream(++index, x, -1);
                return this;
            } catch (Exception e) {
                throw convert(e);
            }
        }

        public void execute() {
            try {
                prep.execute();
            } catch (Exception e) {
                throw convert(e);
            }
        }
    }

    private static Error convert(Exception e) {
        return new Error("Error: " + e.toString(), e);
    }

    public void startTime() {
        start = System.currentTimeMillis();
    }

    public void printTime(String s) {
        System.out.println(s + ": " + (System.currentTimeMillis() - start));
    }
}
