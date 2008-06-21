/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * A condition contains one or two operands and a compare operation.
 * 
 * @param <A> the operand type
 */
/*## Java 1.6 begin ##
public class Condition<A> implements ConditionToken {
    Query< ? > query;
    CompareType compareType;
    A x, y;
    
    Condition(Query< ? > query, A x, A y, CompareType compareType) {
        this.query = query;
        this.compareType = compareType;
        this.x = x;
        this.y = y;
    }
    
    public String toString() {
        if (compareType.hasRightExpression()) {
            return query.getString(x) + compareType.toString() + query.getString(y);
        }
        return query.getString(x) + compareType.toString();
    }
}
## Java 1.6 end ##*/
