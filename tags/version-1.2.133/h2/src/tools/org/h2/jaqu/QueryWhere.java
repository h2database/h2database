/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.5 begin ##
import java.util.List;
//## Java 1.5 end ##

/**
 * This class represents a query with a condition.
 *
 * @param <T> the return type
 */
//## Java 1.5 begin ##
public class QueryWhere<T> {

    Query<T> query;

    QueryWhere(Query<T> query) {
        this.query = query;
    }

    public <A> QueryCondition<T, A> and(A x) {
        query.addConditionToken(ConditionAndOr.AND);
        return new QueryCondition<T, A>(query, x);
    }

    public <A> QueryCondition<T, A> or(A x) {
        query.addConditionToken(ConditionAndOr.OR);
        return new QueryCondition<T, A>(query, x);
    }

    public <X, Z> List<X> select(Z x) {
        return query.select(x);
    }

    public String getSQL() {
        SQLStatement selectList = new SQLStatement(query.getDb());
        selectList.appendSQL("*");
        return query.prepare(selectList, false).getSQL().trim();
    }

    public <X, Z> List<X> selectDistinct(Z x) {
        return query.selectDistinct(x);
    }

    public <X, Z> X selectFirst(Z x) {
        List<X> list = query.select(x);
        return list.isEmpty() ? null : list.get(0);
    }

    public List<T> select() {
        return query.select();
    }

    public T selectFirst() {
        List<T> list = select();
        return list.isEmpty() ? null : list.get(0);
    }

    public List<T> selectDistinct() {
        return query.selectDistinct();
    }

//## Java 1.5 end ##

    /**
     * Order by a number of columns.
     *
     * @param expressions the order by expressions
     * @return the query
     */
//## Java 1.5 begin ##
    public QueryWhere<T> orderBy(Object... expressions) {
        for (Object expr : expressions) {
            OrderExpression<T> e =
                new OrderExpression<T>(query, expr, false, false, false);
            query.addOrderBy(e);
        }
        return this;
    }

    public QueryWhere<T> orderByNullsFirst(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<T>(query, expr, false, true, false);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByNullsLast(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<T>(query, expr, false, false, true);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByDesc(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<T>(query, expr, true, false, false);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByDescNullsFirst(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<T>(query, expr, true, true, false);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByDescNullsLast(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<T>(query, expr, true, false, true);
        query.addOrderBy(e);
        return this;
    }

    public int delete() {
        return query.delete();
    }

    public long selectCount() {
        return query.selectCount();
    }

}
//## Java 1.5 end ##
