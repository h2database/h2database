/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * This class represents a query with a join.
 */
/*## Java 1.6 begin ##
public class QueryJoin {
    
    Query< ? > query;
    
    QueryJoin(Query< ? > query) {
        this.query = query;
    }
    
    public <A> QueryJoinCondition<A> on(A x) {
        return new QueryJoinCondition<A>(query, x);
    }
}
## Java 1.6 end ##*/
