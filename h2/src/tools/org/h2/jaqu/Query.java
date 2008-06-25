/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.h2.jaqu.TableDefinition.FieldDefinition;
import org.h2.jaqu.util.Utils;
//## Java 1.5 end ##

/**
 * This class represents a query.
 *
 * @param <T> the return type
 */
//## Java 1.5 begin ##
public class Query<T> {
    
    private Db db;
    private SelectTable<T> from;
    private ArrayList<ConditionToken> conditions = Utils.newArrayList();
    private ArrayList<SelectTable> joins = Utils.newArrayList();
    private final HashMap<Object, SelectColumn> aliasMap = Utils.newHashMap();
    
    Query(Db db) {
        this.db = db;
    }
    
    static <T> Query<T> from(Db db, T alias) {
        Query<T> query = new Query<T>(db);
        TableDefinition def = db.define(alias.getClass());
        query.from = new SelectTable(db, query, alias, false);
        def.initSelectObject(query.from, alias, query.aliasMap);
        return query;
    }
    
    public List<T> select() {
        List<T> result = Utils.newArrayList();
        ResultSet rs = db.executeQuery(getString());
        try {
            while (rs.next()) {
                T item = from.newObject();
                from.getAliasDefinition().readRow(item, rs);
                result.add(item);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public <X, Z> List<X> select(Z x) {
        Class< ? > clazz = x.getClass();
        if (Utils.isSimpleType(clazz)) {
            return selectSimple((X) x);
        }
        clazz = clazz.getSuperclass();
        return select((Class<X>) clazz, (X) x);
    }
    
    private <X> List<X> select(Class<X> clazz, X x) {
        List<X> result = Utils.newArrayList();
        TableDefinition<X> def = db.define(clazz);
        ResultSet rs = db.executeQuery(getString());
        try {
            while (rs.next()) {
                T item = from.newObject();
                from.getAliasDefinition().readRow(item, rs);
                from.setCurrent(item);
                for (SelectTable s: joins) {
                    Object item2 = s.newObject();
                    s.getAliasDefinition().readRow(item2, rs);
                    s.setCurrent(item2);
                }
                X item2 = Utils.newObject(clazz);
                def.copyAttributeValues(this, item2, x);
                result.add(item2);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    
    private <X> List<X> selectSimple(X x) {
        List<X> result = Utils.newArrayList();
        ResultSet rs = db.executeQuery(getString());
        FieldDefinition<X> def = aliasMap.get(x).getFieldDefinition();
        try {
            while (rs.next()) {
                X item;
                if (def == null) {
                    item = x;
                } else {
                    item = def.read(rs);
                }
                result.add(item);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    
    public <A> QueryCondition<T, A> where(A x) {
        return new QueryCondition<T, A>(this, x);
    }
//## Java 1.5 end ##
    
    /**
     * Order by a number of columns.
     * 
     * @param columns the columns
     * @return the query
     */
//## Java 1.5 begin ##
    public Query<T> orderBy(Integer... columns) {
        return this;
    }
    
    String getString(Object x) {
        SelectColumn col = aliasMap.get(x);
        if (col != null) {
            return col.getString();
        }
        return Utils.quoteSQL(x);
    }

    void addConditionToken(ConditionToken condition) {
        conditions.add(condition);
    }
    
    String getString() {
        StringBuilder buff = new StringBuilder("SELECT * FROM ");
        buff.append(from.getString());
        for (SelectTable join : joins) {
            buff.append(join.getStringAsJoin());
        }
        if (!conditions.isEmpty()) {
            buff.append(" WHERE ");
            for (ConditionToken token : conditions) {
                buff.append(token.getString());
                buff.append(' ');
            }
        }
        return buff.toString();
    }
//## Java 1.5 end ##

    /**
     * Join another table.
     * 
     * @param u an alias for the table to join
     * @return the joined query
     */
//## Java 1.5 begin ##
    public QueryJoin innerJoin(Object alias) {
        TableDefinition def = db.define(alias.getClass());
        SelectTable join = new SelectTable(db, this, alias, false);
        def.initSelectObject(join, alias, aliasMap);
        joins.add(join);
        return new QueryJoin(this, join);
    }

    Db getDb() {
        return db;
    }

    boolean isJoin() {
        return !joins.isEmpty();
    }

    SelectColumn getSelectColumn(Object obj) {
        return aliasMap.get(obj);
    }

}
//## Java 1.5 end ##
