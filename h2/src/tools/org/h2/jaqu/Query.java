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
    private ArrayList<Token> conditions = Utils.newArrayList();
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
    
    public long selectCount() {
        ResultSet rs = db.executeQuery(getSQL("COUNT(*)", false));
        try {
            rs.next();
            long value = rs.getLong(1);
            return value;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> select() {
        return select(false);
    }

    public List<T> selectDistinct() {
        return select(true);
    }
    
    public <X, Z> X selectFirst(Z x) {
        List<X> list = (List<X>) select(x);
        return list.isEmpty() ? null : list.get(0);
    }
    
    public String getSQL() {
        return getSQL("*", false).trim();
    }

    private List<T> select(boolean distinct) {
        List<T> result = Utils.newArrayList();
        ResultSet rs = db.executeQuery(getSQL("*", distinct));
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
    
    public int delete() {
        StringBuilder buff = new StringBuilder();
        buff.append("DELETE FROM ");
        buff.append(from.getString());
        buff.append(getSQLWhere());
        String sql = buff.toString();
        return db.executeUpdate(sql);
    }

    public <X, Z> List<X> selectDistinct(Z x) {
        return select(x, true);
    }
    
    public <X, Z> List<X> select(Z x) {
        return select(x, false);
    }

    private <X, Z> List<X> select(Z x, boolean distinct) {
        Class< ? > clazz = x.getClass();
        if (Utils.isSimpleType(clazz)) {
            return getSimple((X) x, distinct);
        }
        clazz = clazz.getSuperclass();
        return select((Class<X>) clazz, (X) x, distinct);
    }
    
    private <X> List<X> select(Class<X> clazz, X x, boolean distinct) {
        TableDefinition<X> def = db.define(clazz);
        String selectList = def.getSelectList(this, x);
        ResultSet rs = db.executeQuery(getSQL(selectList, distinct));
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
    
    private <X> List<X> getSimple(X x, boolean distinct) {
        String selectList = getString(x);
        ResultSet rs = db.executeQuery(getSQL(selectList, distinct));
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
    
    public QueryWhere<T> whereTrue(Boolean condition) {
        Token token = new Function("", condition);
        addConditionToken(token);
        return new QueryWhere<T>(this);
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
            OrderExpression<Object> e = 
                new OrderExpression<Object>(this, expr, false, false, false);
            addOrderBy(e);
        }
        return this;
    }

    public Query<T> orderByDesc(Object expr) {
        OrderExpression<Object> e = 
            new OrderExpression<Object>(this, expr, true, false, false);
        addOrderBy(e);
        return this;
    }

    public Query<T> groupBy(Object... groupByExpressions) {
        this.groupByExpressions = groupByExpressions;
        return this;
    }

    String getString(Object x) {
        if (x == Function.count()) {
            return "COUNT(*)";
        }
        Token token = Db.getToken(x);
        if (token != null) {
            return token.getString(this);
        }
        SelectColumn col = aliasMap.get(x);
        if (col != null) {
            return col.getString();
        }
        return Utils.quoteSQL(x);
    }

    void addConditionToken(Token condition) {
        conditions.add(condition);
    }
    
    String getSQLWhere() {
        StringBuilder buff = new StringBuilder("");
        if (!conditions.isEmpty()) {
            buff.append(" WHERE ");
            for (Token token : conditions) {
                buff.append(token.getString(this));
                buff.append(' ');
            }
        }
        return buff.toString();
    }
    
    String getSQL(String selectList, boolean distinct) {
        StringBuilder buff = new StringBuilder("SELECT ");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(selectList);
        buff.append(" FROM ");
        buff.append(from.getString());
        for (SelectTable join : joins) {
            buff.append(join.getStringAsJoin(this));
        }
        buff.append(getSQLWhere());
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
     * @param alias an alias for the table to join
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
