/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * An expression to order by in a query.
 * 
 * @param <T> the expression data type
 */
//## Java 1.5 begin ##
class OrderExpression<T> {
    private Query query;
    private T expression;
    private boolean desc;
    private boolean nullsFirst;
    private boolean nullsLast;
    
    OrderExpression(Query query, T expression, boolean desc, boolean nullsFirst, boolean nullsLast) {
        this.query = query;
        this.expression = expression;
        this.desc = desc;
        this.nullsFirst = nullsFirst;
        this.nullsLast = nullsLast;
    }
    
    String getString() {
        StringBuilder buff = new StringBuilder();
        buff.append(query.getString(expression));
        if (desc) {
            buff.append(" DESC");
        }
        if (nullsLast) {
            buff.append(" NULLS LAST");
        }
        if (nullsFirst) {
            buff.append(" NULLS FIRST");
        }
        return buff.toString();
    }
    
}
//## Java 1.5 end ##
