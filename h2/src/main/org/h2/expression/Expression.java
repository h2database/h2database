/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.function.NamedExpression;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.HasSQL;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Typed;
import org.h2.value.Value;

/**
 * An expression is a operation, a value, or a function in a query.
 */
public abstract class Expression implements HasSQL, Typed {

    /**
     * Initial state for {@link #mapColumns(ColumnResolver, int, int)}.
     */
    public static final int MAP_INITIAL = 0;

    /**
     * State for expressions inside a window function for
     * {@link #mapColumns(ColumnResolver, int, int)}.
     */
    public static final int MAP_IN_WINDOW = 1;

    /**
     * State for expressions inside an aggregate for
     * {@link #mapColumns(ColumnResolver, int, int)}.
     */
    public static final int MAP_IN_AGGREGATE = 2;

    /**
     * Wrap expression in parentheses only if it can't be safely included into
     * other expressions without them.
     */
    public static final int AUTO_PARENTHESES = 0;

    /**
     * Wrap expression in parentheses unconditionally.
     */
    public static final int WITH_PARENTHESES = 1;

    /**
     * Do not wrap expression in parentheses.
     */
    public static final int WITHOUT_PARENTHESES = 2;

    private boolean addedToFilter;

    /**
     * Get the SQL snippet for a list of expressions.
     *
     * @param builder the builder to append the SQL to
     * @param expressions the list of expressions
     * @param sqlFlags formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeExpressions(StringBuilder builder, List<? extends Expression> expressions,
            int sqlFlags) {
        for (int i = 0, length = expressions.size(); i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            expressions.get(i).getUnenclosedSQL(builder, sqlFlags);
        }
        return builder;
    }

    /**
     * Get the SQL snippet for an array of expressions.
     *
     * @param builder the builder to append the SQL to
     * @param expressions the list of expressions
     * @param sqlFlags formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeExpressions(StringBuilder builder, Expression[] expressions, int sqlFlags) {
        for (int i = 0, length = expressions.length; i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            Expression e = expressions[i];
            if (e == null) {
                builder.append("DEFAULT");
            } else {
                e.getUnenclosedSQL(builder, sqlFlags);
            }
        }
        return builder;
    }

    /**
     * Return the resulting value for the current row.
     *
     * @param session the session
     * @return the result
     */
    public abstract Value getValue(SessionLocal session);

    /**
     * Returns the data type. The data type may be unknown before the
     * optimization phase.
     *
     * @return the data type
     */
    @Override
    public abstract TypeInfo getType();

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver the column resolver
     * @param level the subquery nesting level
     * @param state current state for nesting checks, initial value is
     *              {@link #MAP_INITIAL}
     */
    public abstract void mapColumns(ColumnResolver resolver, int level, int state);

    /**
     * Try to optimize the expression.
     *
     * @param session the session
     * @return the optimized expression
     */
    public abstract Expression optimize(SessionLocal session);

    /**
     * Try to optimize or remove the condition.
     *
     * @param session the session
     * @return the optimized condition, or {@code null}
     */
    public final Expression optimizeCondition(SessionLocal session) {
        Expression e = optimize(session);
        if (e.isConstant()) {
            return e.getBooleanValue(session) ? null : ValueExpression.FALSE;
        }
        return e;
    }

    /**
     * Tell the expression columns whether the table filter can return values
     * now. This is used when optimizing the query.
     *
     * @param tableFilter the table filter
     * @param value true if the table filter can return value
     */
    public abstract void setEvaluatable(TableFilter tableFilter, boolean value);

    @Override
    public final String getSQL(int sqlFlags) {
        return getSQL(new StringBuilder(), sqlFlags, AUTO_PARENTHESES).toString();
    }

    @Override
    public final StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return getSQL(builder, sqlFlags, AUTO_PARENTHESES);
    }

    /**
     * Get the SQL statement of this expression. This may not always be the
     * original SQL statement, especially after optimization.
     *
     * @param sqlFlags
     *            formatting flags
     * @param parentheses
     *            parentheses mode
     * @return the SQL statement
     */
    public final String getSQL(int sqlFlags, int parentheses) {
        return getSQL(new StringBuilder(), sqlFlags, parentheses).toString();
    }

    /**
     * Get the SQL statement of this expression. This may not always be the
     * original SQL statement, especially after optimization.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @param parentheses
     *            parentheses mode
     * @return the specified string builder
     */
    public final StringBuilder getSQL(StringBuilder builder, int sqlFlags, int parentheses) {
        return parentheses == WITH_PARENTHESES || parentheses != WITHOUT_PARENTHESES && needParentheses()
                ? getUnenclosedSQL(builder.append('('), sqlFlags).append(')')
                : getUnenclosedSQL(builder, sqlFlags);
    }

    /**
     * Returns whether this expressions needs to be wrapped in parentheses when
     * it is used as an argument of other expressions.
     *
     * @return {@code true} if it is
     */
    public boolean needParentheses() {
        return false;
    }

    /**
     * Get the SQL statement of this expression. This may not always be the
     * original SQL statement, especially after optimization. Enclosing '(' and
     * ')' are always appended.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public final StringBuilder getEnclosedSQL(StringBuilder builder, int sqlFlags) {
        return getUnenclosedSQL(builder.append('('), sqlFlags).append(')');
    }

    /**
     * Get the SQL statement of this expression. This may not always be the
     * original SQL statement, especially after optimization. Enclosing '(' and
     * ')' are never appended.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public abstract StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags);

    /**
     * Update an aggregate value. This method is called at statement execution
     * time. It is usually called once for each row, but if the expression is
     * used multiple times (for example in the column list, and as part of the
     * HAVING expression) it is called multiple times - the row counter needs to
     * be used to make sure the internal state is only updated once.
     *
     * @param session the session
     * @param stage select stage
     */
    public abstract void updateAggregate(SessionLocal session, int stage);

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
     * If it is possible, return the negated expression. This is used
     * to optimize NOT expressions: NOT ID&gt;10 can be converted to
     * ID&lt;=10. Returns null if negating is not possible.
     *
     * @param session the session
     * @return the negated expression, or null
     */
    public Expression getNotIfPossible(@SuppressWarnings("unused") SessionLocal session) {
        // by default it is not possible
        return null;
    }

    /**
     * Returns data type of this expression if it is statically known.
     *
     * @param session
     *            the session
     * @return data type or {@code null}
     */
    public TypeInfo getTypeIfStaticallyKnown(SessionLocal session) {
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
     * Check if this expression will always return the NULL value.
     *
     * @return if the expression is constant NULL value
     */
    public boolean isNullConstant() {
        return false;
    }

    /**
     * Is the value of a parameter set.
     *
     * @return true if set
     */
    public boolean isValueSet() {
        return false;
    }

    /**
     * Check if this is an identity column.
     *
     * @return true if it is an identity column
     */
    public boolean isIdentity() {
        return false;
    }

    /**
     * Get the value in form of a boolean expression.
     * Returns true or false.
     * In this database, everything can be a condition.
     *
     * @param session the session
     * @return the result
     */
    public boolean getBooleanValue(SessionLocal session) {
        return getValue(session).isTrue();
    }

    /**
     * Create index conditions if possible and attach them to the table filter.
     *
     * @param session the session
     * @param filter the table filter
     */
    @SuppressWarnings("unused")
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        // default is do nothing
    }

    /**
     * Get the column name or alias name of this expression.
     *
     * @param session the session
     * @param columnIndex 0-based column index
     * @return the column name
     */
    public String getColumnName(SessionLocal session, int columnIndex) {
        return getAlias(session, columnIndex);
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
     * Check whether this expression is a column and can store NULL.
     *
     * @return whether NULL is allowed
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
     * @param session the session
     * @param columnIndex 0-based column index
     * @return the alias name
     */
    public String getAlias(SessionLocal session, int columnIndex) {
        switch (session.getMode().expressionNames) {
        default: {
            String sql = getSQL(QUOTE_ONLY_WHEN_REQUIRED | NO_CASTS, WITHOUT_PARENTHESES);
            if (sql.length() <= Constants.MAX_IDENTIFIER_LENGTH) {
                return sql;
            }
        }
        //$FALL-THROUGH$
        case C_NUMBER:
            return "C" + (columnIndex + 1);
        case EMPTY:
            return "";
        case NUMBER:
            return Integer.toString(columnIndex + 1);
        case POSTGRESQL_STYLE:
            if (this instanceof NamedExpression) {
                return StringUtils.toLowerEnglish(((NamedExpression) this).getName());
            }
            return "?column?";
        }
    }

    /**
     * Get the column name of this expression for a view.
     *
     * @param session the session
     * @param columnIndex 0-based column index
     * @return the column name for a view
     */
    public String getColumnNameForView(SessionLocal session, int columnIndex) {
        switch (session.getMode().viewExpressionNames) {
        case AS_IS:
        default:
            return getAlias(session, columnIndex);
        case EXCEPTION:
            throw DbException.get(ErrorCode.COLUMN_ALIAS_IS_NOT_SPECIFIED_1, getTraceSQL());
        case MYSQL_STYLE: {
            String name = getSQL(QUOTE_ONLY_WHEN_REQUIRED | NO_CASTS, WITHOUT_PARENTHESES);
            if (name.length() > 64) {
                name = "Name_exp_" + (columnIndex + 1);
            }
            return name;
        }
        }
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
     */
    public void addFilterConditions(TableFilter filter) {
        if (!addedToFilter && isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
            filter.addFilterCondition(this, false);
            addedToFilter = true;
        }
    }

    /**
     * Convert this expression to a String.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return getTraceSQL();
    }

    /**
     * Returns count of subexpressions.
     *
     * @return count of subexpressions
     */
    public int getSubexpressionCount() {
        return 0;
    }

    /**
     * Returns subexpression with specified index.
     *
     * @param index 0-based index
     * @return subexpression with specified index, may be null
     * @throws IndexOutOfBoundsException if specified index is not valid
     */
    public Expression getSubexpression(int index) {
        throw new IndexOutOfBoundsException();
    }

    /**
     * Return the resulting value of when operand for the current row.
     *
     * @param session
     *            the session
     * @param left
     *            value on the left side
     * @return the result
     */
    public boolean getWhenValue(SessionLocal session, Value left) {
        return session.compareWithNull(left, getValue(session), true) == 0;
    }

    /**
     * Appends the SQL statement of this when operand to the specified builder.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        return getUnenclosedSQL(builder.append(' '), sqlFlags);
    }

    /**
     * Returns whether this expression is a right side of condition in a when
     * operand.
     *
     * @return {@code true} if it is, {@code false} otherwise
     */
    public boolean isWhenConditionOperand() {
        return false;
    }

}
