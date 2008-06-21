/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.6 begin ##
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.h2.jaqu.TableDefinition.FieldDefinition;
import org.h2.jaqu.util.Utils;
import org.h2.jaqu.util.WeakIdentityHashMap;
import org.h2.util.JdbcUtils;
//## Java 1.6 end ##

/**
 * This class represents a connection to a database.
 */
//## Java 1.6 begin ##
public class Db {
    
    private final Connection conn;
    private final Map<Class, TableDefinition> classMap = Utils.newHashMap();
    private final WeakIdentityHashMap<Object, FieldDefinition> aliasMap = 
            Utils.newWeakIdentityHashMap();
    
    Db(Connection conn) {
        this.conn = conn;
    }
    
    public static <T> T instance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new Error(e);
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

    public <T extends Object> Query<T> from(T alias) {
        return new Query<T>(this, alias);
    }
    
    public <T> void createTable(Class<T> clazz) {
        define(clazz).createTableIfRequired(this);
    }
    
    <T> TableDefinition<T> define(Class<T> clazz) {
        TableDefinition def = classMap.get(clazz);
        if (def == null) {
            def = new TableDefinition(clazz);
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

    public <T> T alias(Class<T> clazz) {
        TableDefinition def = define(clazz);
        T alias = instance(clazz);
        def.initObject(alias, aliasMap);
        return alias;
    }

    public void close() {
        try {
            conn.close();
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public <T> void insertAll(List<T> list) {
        for (T t : list) {
            insert(t);
        }
    }

    void execute(String sql) {
        try {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public PreparedStatement prepare(String sql) {
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    TableDefinition getTableDefinition(Class< ? > clazz) {
        return classMap.get(clazz);
    }

    public ResultSet executeQuery(String sql) {
        try {
            return conn.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <X> FieldDefinition<X> getFieldDefinition(X x) {
        return aliasMap.get(x);
    }

}
//## Java 1.6 end ##
