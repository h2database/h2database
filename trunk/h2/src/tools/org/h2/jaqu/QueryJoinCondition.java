/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * This class represents a query with join and an incomplete condition.
 *
 * @param <A> the incomplete condition data type
 */
//## Java 1.6 begin ##
public class QueryJoinCondition<A> {
    
    private Query< ? > query;
    private A x;

    public QueryJoinCondition(Query< ? > query, A x) {
        this.query = query;
        this.x = x;
    }
    
    public Query< ? > is(A y) {
        query.addConditionToken(new Condition<A>(query, x, y, CompareType.EQUAL));
        return query;
    }
}
//## Java 1.6 end ##
