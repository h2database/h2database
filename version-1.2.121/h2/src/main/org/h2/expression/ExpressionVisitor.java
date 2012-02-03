/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
     * Are all aggregates MIN(column), MAX(column), or COUNT(*) for the given
     * table (getTable)?
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
     * Does an expression have no relation to the given table filter
     * (getResolver)?
     */
    public static final int NOT_FROM_RESOLVER = 6;

    /**
     * Request to get the set of dependencies.
     */
    public static final int GET_DEPENDENCIES = 7;

    private int queryLevel;
    private Table table;
    private int type;
    private long maxDataModificationId;
    private ColumnResolver resolver;
    private HashSet<DbObject> dependencies;

    private ExpressionVisitor(int type) {
        this.type = type;
    }

    /**
     * Create a new visitor object with the given type.
     *
     * @param type the visitor type
     * @return the new visitor
     */
    public static ExpressionVisitor get(int type) {
        return new ExpressionVisitor(type);
    }

    /**
     * Add a new dependency to the set of dependencies.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @param obj the additional dependency.
     */
    public void addDependency(DbObject obj) {
        dependencies.add(obj);
    }

    /**
     * Get the dependency set.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @return the set
     */
    public HashSet<DbObject> getDependencies() {
        return dependencies;
    }

    /**
     * Set all dependencies.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @param dependencies the dependency set
     */
    public void setDependencies(HashSet<DbObject> dependencies) {
        this.dependencies = dependencies;
    }

    /**
     * Increment or decrement the query level.
     *
     * @param offset 1 to increment, -1 to decrement
     */
    public void incrementQueryLevel(int offset) {
        queryLevel += offset;
    }

    /**
     * Get the column resolver.
     * This is used for NOT_FROM_RESOLVER visitors.
     *
     * @return the column resolver
     */
    public ColumnResolver getResolver() {
        return resolver;
    }

    /**
     * Set the column resolver.
     * This is used for NOT_FROM_RESOLVER visitors.
     *
     * @param resolver the column resolver
     */
    public void setResolver(ColumnResolver resolver) {
        this.resolver = resolver;
    }

    /**
     * Update the field maxDataModificationId if this value is higher
     * than the current value.
     * This is used for SET_MAX_DATA_MODIFICATION_ID visitors.
     *
     * @param value the data modification id
     */
    public void addDataModificationId(long value) {
        maxDataModificationId = Math.max(maxDataModificationId, value);
    }

    /**
     * Get the last data modification.
     * This is used for SET_MAX_DATA_MODIFICATION_ID visitors.
     *
     * @return the maximum modification id
     */
    public long getMaxDataModificationId() {
        return maxDataModificationId;
    }

    int getQueryLevel() {
        return queryLevel;
    }

    void setQueryLevel(int queryLevel) {
        this.queryLevel = queryLevel;
    }

    /**
     * Set the table.
     * This is used for OPTIMIZABLE_MIN_MAX_COUNT_ALL visitors.
     *
     * @param table the table
     */
    public void setTable(Table table) {
        this.table = table;
    }

    /**
     * Get the table.
     * This is used for OPTIMIZABLE_MIN_MAX_COUNT_ALL visitors.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get the visitor type.
     *
     * @return the type
     */
    public int getType() {
        return type;
    }

}
