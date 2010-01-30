/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.h2.jaqu.util.Utils;
import org.h2.jaqu.util.WeakIdentityHashMap;
import org.h2.util.JdbcUtils;
//## Java 1.5 end ##

/**
 * This class represents a connection to a database.
 */
//## Java 1.5 begin ##
public class Db {

    private static final WeakIdentityHashMap<Object, Token> TOKENS =
        Utils.newWeakIdentityHashMap();

    private final Connection conn;
    private final Map<Class< ? >, TableDefinition< ? >> classMap =
        Utils.newHashMap();

    Db(Connection conn) {
        this.conn = conn;
    }

    static <X> X registerToken(X x, Token token) {
        TOKENS.put(x, token);
        return x;
    }

    static Token getToken(Object x) {
        return TOKENS.get(x);
    }

    private static <T> T instance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Db open(String url, String user, String password) {
        try {
            Connection conn = JdbcUtils.getConnection(null, url, user, password);
            return new Db(conn);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    public static Db open(String url, String user, char[] password) {
        try {
            Properties prop = new Properties();
            prop.setProperty("user", user);
            prop.put("password", password);
            Connection conn = JdbcUtils.getConnection(null, url, prop);
            return new Db(conn);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    private static Error convert(Exception e) {
        return new Error(e);
    }

    public <T> void insert(T t) {
        Class< ? > clazz = t.getClass();
        define(clazz).createTableIfRequired(this).insert(this, t);
    }

    public <T> void merge(T t) {
        Class< ? > clazz = t.getClass();
        define(clazz).createTableIfRequired(this).merge(this, t);
    }

    public <T> void update(T t) {
        Class< ? > clazz = t.getClass();
        define(clazz).createTableIfRequired(this).update(this, t);
    }

    public <T extends Object> Query<T> from(T alias) {
        Class< ? > clazz = alias.getClass();
        define(clazz).createTableIfRequired(this);
        return Query.from(this, alias);
    }

    <T> void createTable(Class<T> clazz) {
        define(clazz).createTableIfRequired(this);
    }

    <T> TableDefinition<T> define(Class<T> clazz) {
        TableDefinition<T> def = getTableDefinition(clazz);
        if (def == null) {
            def = new TableDefinition<T>(clazz);
            def.mapFields();
            classMap.put(clazz, def);
            if (Table.class.isAssignableFrom(clazz)) {
                T t = instance(clazz);
                Table table = (Table) t;
                Define.define(def, table);
            }
        }
        return def;
    }

    public void close() {
        try {
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <A> TestCondition<A> test(A x) {
        return new TestCondition<A>(x);
    }

    public <T> void insertAll(List<T> list) {
        for (T t : list) {
            insert(t);
        }
    }

    PreparedStatement prepare(String sql) {
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    <T> TableDefinition<T> getTableDefinition(Class<T> clazz) {
        return (TableDefinition<T>) classMap.get(clazz);
    }

    /**
     * Run a SQL query directly against the database.
     *
     * @param sql the SQL statement
     * @return the result set
     */
    public ResultSet executeQuery(String sql) {
        try {
            return conn.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run a SQL statement directly against the database.
     *
     * @param sql the SQL statement
     * @return the update count
     */
    public int executeUpdate(String sql) {
        try {
            Statement stat = conn.createStatement();
            int updateCount = stat.executeUpdate(sql);
            stat.close();
            return updateCount;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    <X> FieldDefinition<X> getFieldDefinition(X x) {
//        return aliasMap.get(x).getFieldDefinition();
//    }
//
//    <X> SelectColumn<X> getSelectColumn(X x) {
//        return aliasMap.get(x);
//    }

}
//## Java 1.5 end ##
