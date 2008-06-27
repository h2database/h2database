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
    private ArrayList<OrderExpression> orderByList = Utils.newArrayList();
    private Object[] groupByExpressions;
    
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
    
    public long selectCountStar() {
        ResultSet rs = db.executeQuery(getString("COUNT(*)"));
        try {
            rs.next();
            long value = rs.getLong(1);
            return value;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> select() {
        List<T> result = Utils.newArrayList();
        ResultSet rs = db.executeQuery(getString("*"));
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
        TableDefinition<X> def = db.define(clazz);
        String selectList = def.getSelectList(this, x);
        ResultSet rs = db.executeQuery(getString(selectList));
        List<X> result = Utils.newArrayList();
        try {
            while (rs.next()) {
                X row = Utils.newObject(clazz);
                def.readRow(row, rs);
                result.add(row);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    
    private <X> List<X> selectSimple(X x) {
        String selectList = getString(x);
        ResultSet rs = db.executeQuery(getString(selectList));
        List<X> result = Utils.newArrayList();
        try {
            while (rs.next()) {
                try {
                    X value = (X) rs.getObject(1);
                    result.add(value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
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
     * @param expressions the columns
     * @return the query
     */
//## Java 1.5 begin ##
    public Query<T> orderBy(Object... expressions) {
        for (Object expr : expressions) {
            OrderExpression<Object> e = new OrderExpression<Object>(this, expr, false, false, false);
            addOrderBy(e);
        }
        return this;
    }

    public Query<T> orderByNullsFirst(Object expr) {
        OrderExpression<Object> e = new OrderExpression<Object>(this, expr, false, true, false);
        addOrderBy(e);
        return this;
    }

    public Query<T> orderByNullsLast(Object expr) {
        OrderExpression<Object> e = new OrderExpression<Object>(this, expr, false, false, true);
        addOrderBy(e);
        return this;
    }

    public Query<T> orderByDesc(Object expr) {
        OrderExpression<Object> e = new OrderExpression<Object>(this, expr, true, false, false);
        addOrderBy(e);
        return this;
    }

    public Query<T> orderByDescNullsFirst(Object expr) {
        OrderExpression<Object> e = new OrderExpression<Object>(this, expr, true, true, false);
        addOrderBy(e);
        return this;
    }

    public Query<T> orderByDescNullsLast(Object expr) {
        OrderExpression<Object> e = new OrderExpression<Object>(this, expr, true, false, true);
        addOrderBy(e);
        return this;
    }
    
    public Query<T> groupBy(Object... groupByExpressions) {
        this.groupByExpressions = groupByExpressions;
        return this;
    }

    String getString(Object x) {
        if (x == Function.countStar()) {
            return "COUNT(*)";
        }
        SelectColumn col = aliasMap.get(x);
        if (col != null) {
            return col.getString();
        }
        return Utils.quoteSQL(x);
    }

    void addConditionToken(ConditionToken condition) {
        conditions.add(condition);
    }
    
    String getString(String selectList) {
        StringBuilder buff = new StringBuilder("SELECT ");
        buff.append(selectList);
        buff.append(" FROM ");
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
        if (groupByExpressions != null) {
            buff.append(" GROUP BY ");
            for (int i = 0; i < groupByExpressions.length; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                Object obj = groupByExpressions[i];
                buff.append(getString(obj));
                buff.append(' ');
            }
        }
        if (!orderByList.isEmpty()) {
            buff.append(" ORDER BY ");
            for (int i = 0; i < orderByList.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                OrderExpression o = orderByList.get(i);
                buff.append(o.getString());
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

    void addOrderBy(OrderExpression expr) {
        orderByList.add(expr);
    }

}
//## Java 1.5 end ##
