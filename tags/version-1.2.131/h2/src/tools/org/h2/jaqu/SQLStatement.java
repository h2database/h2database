/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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
public class SQLStatement {
    private Db db;
    private StringBuilder buff = new StringBuilder();
    private String sql;
    private ArrayList<Object> params = new ArrayList<Object>();

    SQLStatement(Db db) {
        this.db = db;
    }

    void setSQL(String sql) {
        this.sql = sql;
        buff = new StringBuilder(sql);
    }

    public SQLStatement appendSQL(String s) {
        buff.append(s);
        sql = null;
        return this;
    }

    String getSQL() {
        if (sql == null) {
            sql = buff.toString();
        }
        return sql;
    }

    SQLStatement addParameter(Object o) {
        params.add(o);
        return this;
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
        PreparedStatement prep = db.prepare(getSQL());
        for (int i = 0; i < params.size(); i++) {
            Object o = params.get(i);
            setValue(prep, i + 1, o);
        }
        return prep;
    }

}
//## Java 1.5 end ##
