/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
//## Java 1.5 end ##

/**
 * This class represents a parameterized SQL statement.
 */
//## Java 1.5 begin ##
public class SqlStatement {
    private Db db;
    private String sql = "";
    private ArrayList params = new ArrayList();

    SqlStatement(Db db) {
        this.db = db;
    }

    void setSQL(String sql) {
        this.sql = sql;
    }

    void appendSQL(String s) {
        sql += s;
    }

    String getSQL() {
        return sql;
    }

    void addParameter(Object o) {
        params.add(o);
    }

    ResultSet executeQuery() {
        try {
            return prepare().executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    int executeUpdate() {
        try {
            return prepare().executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setValue(PreparedStatement prep, int parameterIndex, Object x) {
        try {
            prep.setObject(parameterIndex, x);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private PreparedStatement prepare() {
        PreparedStatement prep = db.prepare(sql);
        for (int i = 0; i < params.size(); i++) {
            Object o = params.get(i);
            setValue(prep, i + 1, o);
        }
        return prep;
    }

}
//## Java 1.5 end ##
