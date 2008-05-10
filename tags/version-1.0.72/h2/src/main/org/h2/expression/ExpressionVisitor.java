/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.HashSet;

import org.h2.engine.DbObject;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;

/**
 * The visitor pattern is used to iterate through all expressions of a query
 * to optimize a statement.
 */
public class ExpressionVisitor {
    
    /**
     * Is the value independent on unset parameters or on columns of a higher
     * level query, or sequence values (that means can it be evaluated right
     * now)?
     */
    public static final int INDEPENDENT = 0;

    /**
     * Are all aggregates MIN(column), MAX(column), or COUNT(*)?
     */
    public static final int OPTIMIZABLE_MIN_MAX_COUNT_ALL = 1;

    /**
     * Does the expression return the same results for the same parameters?
     */
    public static final int DETERMINISTIC = 2;

    /**
     * Can the expression be evaluated, that means are all columns set to
     * 'evaluatable'?
     */
    public static final int EVALUATABLE = 3;

    /**
     * Request to set the latest modification id.
     */
    public static final int SET_MAX_DATA_MODIFICATION_ID = 4;

    /**
     * Does the expression have no side effects (change the data)?
     */
    public static final int READONLY = 5;

    /**
     * Does an expression have no relation to the given table filter?
     */
    public static final int NOT_FROM_RESOLVER = 6;

    /**
     * Request to get the set of dependencies.
     */
    public static final int GET_DEPENDENCIES = 7;

    int queryLevel;
    public Table table;
    public int type;
    private long maxDataModificationId;
    private ColumnResolver resolver;
    private HashSet dependencies;

    public static ExpressionVisitor get(int type) {
        return new ExpressionVisitor(type);
    }

    public long getMaxDataModificationId() {
        return maxDataModificationId;
    }

    public void addDependency(DbObject obj) {
        dependencies.add(obj);
    }

    public HashSet getDependencies() {
        return dependencies;
    }

    public void setDependencies(HashSet dependencies) {
        this.dependencies = dependencies;
    }

    private ExpressionVisitor(int type) {
        this.type = type;
    }

    public void queryLevel(int offset) {
        queryLevel += offset;
    }

    public ColumnResolver getResolver() {
        return resolver;
    }

    public void addDataModificationId(long v) {
        maxDataModificationId = Math.max(maxDataModificationId, v);
    }

    public static ExpressionVisitor getNotFromResolver(ColumnResolver resolver) {
        ExpressionVisitor v = new ExpressionVisitor(NOT_FROM_RESOLVER);
        v.resolver = resolver;
        return v;
    }

}
