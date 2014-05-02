/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.h2.util.New;

/**
 * Represents a connection to a real database.
 */
class DbConnection implements DbInterface {
    private TestSynth config;
    private int id;
    private String driver;
    private String url;
    private String user;
    private String password;
    private Connection conn;
    private Connection sentinel;
    private boolean useSentinel;

    DbConnection(TestSynth config, String driver, String url, String user, String password, int id, boolean useSentinel) {
        this.config = config;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.id = id;
        this.useSentinel = useSentinel;
        log("url=" + url);
    }

    public void reset() throws SQLException {
        log("reset;");
        DatabaseMetaData meta = conn.getMetaData();
        Statement stat = conn.createStatement();
        ArrayList<String> tables = New.arrayList();
        ResultSet rs = meta.getTables(null, null, null, new String[] { "TABLE" });
        while (rs.next()) {
            String schemaName = rs.getString("TABLE_SCHEM");
            if (!"INFORMATION_SCHEMA".equals(schemaName)) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        while (tables.size() > 0) {
            int dropped = 0;
            for (int i = 0; i < tables.size(); i++) {
                try {
                    String table = tables.get(i);
                    stat.execute("DROP TABLE " + table);
                    dropped++;
                    tables.remove(i);
                    i--;
                } catch (SQLException e) {
                    // maybe a referential integrity
                }
            }
            // could not drop any table and still tables to drop
            if (dropped == 0 && tables.size() > 0) {
                throw new AssertionError("Cannot drop " + tables);
            }
        }
    }

    public void connect() throws Exception {
        if (useSentinel && sentinel == null) {
            sentinel = getConnection();
        }
        log("connect to " + url + ";");
        conn = getConnection();
    }

    private Connection getConnection() throws Exception {
        log("(getConnection to " + url + ");");
        if (driver == null) {
            return config.getConnection("synth");
        }
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }

    public void disconnect() throws SQLException {
        log("disconnect " + url + ";");
        conn.close();
    }

    public void end() throws SQLException {
        log("end " + url + ";");
        if (sentinel != null) {
            sentinel.close();
            sentinel = null;
        }
    }

    public void createTable(Table table) throws SQLException {
        execute(table.getCreateSQL());
    }

    public void dropTable(Table table) throws SQLException {
        execute(table.getDropSQL());
    }

    public void createIndex(Index index) throws SQLException {
        execute(index.getCreateSQL());
        index.getTable().addIndex(index);
    }

    public void dropIndex(Index index) throws SQLException {
        execute(index.getDropSQL());
        index.getTable().removeIndex(index);
    }

    public Result insert(Table table, Column[] c, Value[] v) throws SQLException {
        String sql = table.getInsertSQL(c, v);
        execute(sql);
        return new Result(sql, 1);
    }

    private void execute(String sql) throws SQLException {
        log(sql + ";");
        conn.createStatement().execute(sql);
    }

    public Result select(String sql) throws SQLException {
        log(sql + ";");
        Statement stat = conn.createStatement();
        Result result = new Result(config, sql, stat.executeQuery(sql));
        return result;
    }

    public Result delete(Table table, String condition) throws SQLException {
        String sql = "DELETE FROM " + table.getName();
        if (condition != null) {
            sql += "  WHERE " + condition;
        }
        log(sql + ";");
        Statement stat = conn.createStatement();
        Result result = new Result(sql, stat.executeUpdate(sql));
        return result;
    }

    public Result update(Table table, Column[] columns, Value[] values, String condition) throws SQLException {
        String sql = "UPDATE " + table.getName() + " SET ";
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sql += ", ";
            }
            sql += columns[i].getName() + "=" + values[i].getSQL();
        }
        if (condition != null) {
            sql += "  WHERE " + condition;
        }
        log(sql + ";");
        Statement stat = conn.createStatement();
        Result result = new Result(sql, stat.executeUpdate(sql));
        return result;
    }

    public void setAutoCommit(boolean b) throws SQLException {
        log("set autoCommit " + b + ";");
        conn.setAutoCommit(b);
    }

    public void commit() throws SQLException {
        log("commit;");
        conn.commit();
    }

    public void rollback() throws SQLException {
        log("rollback;");
        conn.rollback();
    }

    private void log(String s) {
        config.log(id, s);
    }

    public String toString() {
        return url;
    }

}
