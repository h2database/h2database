/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.dml.Select;
import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * An expression is a operation, a value, or a function in a query.
 */
public abstract class Expression {

    private boolean addedToFilter;

    /**
     * Return the resulting value for the current row.
     *
     * @param session the session
     * @return the result
     */
    public abstract Value getValue(Session session) throws SQLException;

    /**
     * Return the data type. The data type may not be known before the
     * optimization phase.
     *
     * @return the type
     */
    public abstract int getType();

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver the column resolver
     * @param level the subquery nesting level
     */
    public abstract void mapColumns(ColumnResolver resolver, int level) throws SQLException;

    /**
     * Try to optimize the expression.
     *
     * @param session the session
     * @return the optimized expression
     */
    public abstract Expression optimize(Session session) throws SQLException;

    /**
     * Tell the expression columns whether the table filter can return values now.
     * This is used when optimizing the query.
     *
     * @param tableFilter the table filter
     * @param value true if the table filter can return value
     */
    public abstract void setEvaluatable(TableFilter tableFilter, boolean value);

    /**
     * Get the scale of this expression.
     *
     * @return the scale
     */
    public abstract int getScale();

    /**
     * Get the precision of this expression.
     *
     * @return the precision
     */
    public abstract long getPrecision();

    /**
     * Get the display size of this expression.
     *
     * @return the display size
     */
    public abstract int getDisplaySize();

    /**
     * Get the SQL statement of this expression.
     * This may not always be the original SQL statement,
     * specially after optimization.
     *
     * @return the SQL statement
     */
    public abstract String getSQL();

    /**
     * Update an aggregate value.
     * This method is called at statement execution time.
     * It is usually called once for each row, but if the expression is used multiple
     * times (for example in the column list, and as part of the HAVING expression)
     * it is called multiple times - the row counter needs to be used to make sure
     * the internal state is only updated once.
     *
     * @param session the session
     */
    public abstract void updateAggregate(Session session) throws SQLException;

    /**
     * Check if this expression and all sub-expressions can fulfill a criteria.
     * If any part returns false, the result is false.
     *
     * @param visitor the visitor
     * @return if the criteria can be fulfilled
     */
    public abstract boolean isEverything(ExpressionVisitor visitor);

    /**
     * Estimate the cost to process the expression.
     * Used when optimizing the query, to calculate the query plan
     * with the lowest estimated cost.
     *
     * @return the estimated cost
     */
    public abstract int getCost();

    /**
     * Check if this expression and all sub-expressions can fulfill a criteria.
     * This is a convenience function.
     *
     * @param expressionVisitorType the visitor type
     * @return if the criteria can be fulfilled
     */
    public final boolean isEverything(int expressionVisitorType) {
        ExpressionVisitor visitor = ExpressionVisitor.get(expressionVisitorType);
        return isEverything(visitor);
    }

    /**
     * If it is possible, return the negated expression. This is used
     * to optimize NOT expressions: NOT ID>10 can be converted to
     * ID&lt;=10. Returns null if negating is not possible.
     *
     * @param session the session
     * @return the negated expression, or null
     */
    public Expression getNotIfPossible(Session session) {
        // by default it is not possible
        return null;
    }

    /**
     * Check if this expression will always return the same value.
     *
     * @return if the expression is constant
     */
    public boolean isConstant() {
        return false;
    }

    /**
     * Is the value of a parameter set.
     *
     * @return if it is set
     */
    public boolean isValueSet() {
        return false;
    }

    /**
     * Check if this is an auto-increment column.
     *
     * @return true if it is an auto-increment column
     */
    public boolean isAutoIncrement() {
        return false;
    }

    /**
     * Get the value in form of a boolean expression.
     * Returns true, false, or null.
     * In this database, everything can be a condition.
     *
     * @param session the session
     * @return the result
     */
    public Boolean getBooleanValue(Session session) throws SQLException {
        return getValue(session).getBoolean();
    }

    /**
     * Create index conditions if possible and attach them to the table filter.
     *
     * @param session the session
     * @param filter the table filter
     * @throws SQLException
     */
    public void createIndexConditions(Session session, TableFilter filter) throws SQLException {
        // default is do nothing
    }

    /**
     * Get the column name or alias name of this expression.
     *
     * @return the column name
     */
    public String getColumnName() {
        return getAlias();
    }

    /**
     * Get the schema name, or null
     *
     * @return the schema name
     */
    public String getSchemaName() {
        return null;
    }

    /**
     * Get the table name, or null
     *
     * @return the table name
     */
    public String getTableName() {
        return null;
    }

    /**
     * Check whether this expression is a column and can store null values.
     *
     * @return whether null values are allowed
     */
    public int getNullable() {
        return Column.NULLABLE_UNKNOWN;
    }

    /**
     * Get the table alias name or null
     * if this expression does not represent a column.
     *
     * @return the table alias name
     */
    public String getTableAlias() {
        return null;
    }

    /**
     * Get the alias name of a column or SQL expression
     * if it is not an aliased expression.
     *
     * @return the alias name
     */
    public String getAlias() {
        return StringUtils.unEnclose(getSQL());
    }

    /**
     * Only returns true if the expression is a wildcard.
     *
     * @return if this expression is a wildcard
     */
    public boolean isWildcard() {
        return false;
    }

    /**
     * Returns the main expression, skipping aliases.
     *
     * @return the expression
     */
    public Expression getNonAliasExpression() {
        return this;
    }

    /**
     * Add conditions to a table filter if they can be evaluated.
     *
     * @param filter the table filter
     * @param outerJoin if the expression is part of an outer join
     */
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (!addedToFilter && !outerJoin && isEverything(ExpressionVisitor.EVALUATABLE)) {
            filter.addFilterCondition(this, false);
            addedToFilter = true;
        }
    }

    /**
     * Convert this expression to a String.
     *
     * @return the string representation
     */
    public String toString() {
        return getSQL();
    }

    /**
     * Optimize IN(...) expressions if possible.
     *
     * @param session the session
     * @param select the query
     * @return the optimized expression
     * @throws SQLException
     */
    public Expression optimizeInJoin(Session session, Select select) throws SQLException {
        return this;
    }

}
