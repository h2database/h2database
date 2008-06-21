/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

//## Java 1.6 begin ##
import java.util.List;
//## Java 1.6 end ##

/**
 * This class represents a query with a condition.
 *
 * @param <T> the return type
 */
//## Java 1.6 begin ##
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
        return (List<X>) query.select(x);
    }

    public List<T> select() {
        return query.select();
    }
//## Java 1.6 end ##

    /**
     * Order by a number of columns.
     * 
     * @param columns the columns
     * @return the query
     */
//## Java 1.6 begin ##
    public QueryWhere<T> orderBy(Integer... columns) {
        return this;
    }

}
//## Java 1.6 end ##
