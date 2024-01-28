/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.h2.command.query.Query;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.expression.condition.Comparison;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueRow;

import static org.h2.util.HasSQL.TRACE_SQL_FLAGS;

/**
 * An index condition object is made for each condition that can potentially use
 * an index. This class does not extend expression, but in general there is one
 * expression that maps to each index condition.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class IndexCondition {

    /**
     * A bit of a search mask meaning 'equal'.
     */
    public static final int EQUALITY = 1;

    /**
     * A bit of a search mask meaning 'larger or equal'.
     */
    public static final int START = 2;

    /**
     * A bit of a search mask meaning 'smaller or equal'.
     */
    public static final int END = 4;

    /**
     * A search mask meaning 'between'.
     */
    public static final int RANGE = START | END;

    /**
     * A bit of a search mask meaning 'the condition is always false'.
     */
    public static final int ALWAYS_FALSE = 8;

    /**
     * A bit of a search mask meaning 'spatial intersection'.
     */
    public static final int SPATIAL_INTERSECTS = 16;

    private final Column column;
    private final Column[] columns;
    private final boolean compoundColumns;

    /**
     * see constants in {@link Comparison}
     */
    private final int compareType;

    private final Expression expression;
    private final List<Expression> expressionList;
    private final Query expressionQuery;

    /**
     * @param compareType the comparison type, see constants in
     *            {@link Comparison}
     */
    private IndexCondition(int compareType, ExpressionColumn column, Column[] columns, Expression expression,
            List<Expression> list, Query query) {

        this.compareType = compareType;
        if (column != null) {
            this.column = column.getColumn();
            this.columns = null;
            this.compoundColumns = false;
        } else if (columns !=null) {
            this.column = null;
            this.columns = columns;
            this.compoundColumns = true;
        } else {
            this.column = null;
            this.columns = null;
            this.compoundColumns = false;
        }
        this.expression = expression;
        this.expressionList = list;
        this.expressionQuery = query;
    }

    /**
     * Create an index condition with the given parameters.
     *
     * @param compareType the comparison type, see constants in {@link Comparison}
     * @param column the column
     * @param expression the expression
     * @return the index condition
     */
    public static IndexCondition get(int compareType, ExpressionColumn column, Expression expression) {
        return new IndexCondition(compareType, column, null, expression, null, null);
    }

    /**
     * Create an index condition with the compare type IN_LIST and with the given parameters.
     *
     * @param column the column
     * @param list the expression list
     * @return the index condition
     */
    public static IndexCondition getInList(ExpressionColumn column, List<Expression> list) {
        return new IndexCondition(Comparison.IN_LIST, column, null, null, list, null);
    }

    /**
     * Create a compound index condition with the compare type IN_LIST and with the given parameters.
     *
     * @param columns the columns
     * @param list the expression list
     * @return the index condition
     */
    public static IndexCondition getCompoundInList(ExpressionList columns, List<Expression> list) {
        int listSize = columns.getSubexpressionCount();
        Column[] cols = new Column[listSize];
        for (int i = listSize; --i >= 0; ) {
            cols[i] = ((ExpressionColumn) columns.getSubexpression(i)).getColumn();
        }

        return new IndexCondition(Comparison.IN_LIST, null, cols, null, list, null);
    }

    /**
     * Create an index condition with the compare type IN_ARRAY and with the given parameters.
     *
     * @param column the column
     * @param array the array
     * @return the index condition
     */
    public static IndexCondition getInArray(ExpressionColumn column, Expression array) {
        return new IndexCondition(Comparison.IN_ARRAY, column, null, array, null, null);
    }

    /**
     * Create an index condition with the compare type IN_QUERY and with the given parameters.
     *
     * @param column the column
     * @param query the select statement
     * @return the index condition
     */
    public static IndexCondition getInQuery(ExpressionColumn column, Query query) {
        assert query.isRandomAccessResult();
        return new IndexCondition(Comparison.IN_QUERY, column, null, null, null, query);
    }

    /**
     * Get the current value of the expression.
     *
     * @param session the session
     * @return the value
     */
    public Value getCurrentValue(SessionLocal session) {
        return expression.getValue(session);
    }

    /**
     * Get the current value list of the expression. The value list is of the
     * same type as the column, distinct, and sorted.
     *
     * @param session the session
     * @return the value list
     */
    public Value[] getCurrentValueList(SessionLocal session) {
        TreeSet<Value> valueSet = new TreeSet<>(session.getDatabase().getCompareMode());
        if (compareType == Comparison.IN_LIST) {
            if (isCompoundColumns()) {
                Column[] columns = getColumns();
                for (Expression e : expressionList) {
                    ValueRow v = (ValueRow) e.getValue(session);
                    v = Column.convert(session, columns, v);
                    valueSet.add(v);
                }
            }
            else {
                Column column = getColumn();
                for (Expression e : expressionList) {
                    Value v = e.getValue(session);
                    v = column.convert(session, v);
                    valueSet.add(v);
                }
            }
        } else if (compareType == Comparison.IN_ARRAY) {
            Value v = expression.getValue(session);
            if (v instanceof ValueArray) {
                for (Value e : ((ValueArray) v).getList()) {
                    valueSet.add(e);
                }
            }
        } else {
            throw DbException.getInternalError("compareType = " + compareType);
        }
        Value[] array = valueSet.toArray(new Value[valueSet.size()]);
        Arrays.sort(array, session.getDatabase().getCompareMode());
        return array;
    }

    /**
     * Get the current result of the expression. The rows may not be of the same
     * type, therefore the rows may not be unique.
     *
     * @return the result
     */
    public ResultInterface getCurrentResult() {
        return expressionQuery.query(0);
    }

    /**
     * Get the SQL snippet of this comparison.
     *
     * @param sqlFlags formatting flags
     * @return the SQL snippet
     */
    public String getSQL(int sqlFlags) {
        if (compareType == Comparison.FALSE) {
            return "FALSE";
        }
        StringBuilder builder = new StringBuilder();
        builder = isCompoundColumns() ? buildSql(sqlFlags, builder) : buildSql(sqlFlags, getColumn(), builder);
        return builder.toString();
    }

    private StringBuilder buildSql(int sqlFlags, StringBuilder builder) {
        if (compareType == Comparison.IN_LIST) {
            builder.append(" IN(");
            for (int i = 0, s = expressionList.size(); i < s; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(expressionList.get(i).getSQL(sqlFlags));
            }
            return builder.append(')');
        }
        else {
            throw DbException.getInternalError("Multiple columns can only be used with compound IN lists.");
        }
    }

    private StringBuilder buildSql(int sqlFlags, Column column, StringBuilder builder) {
        column.getSQL(builder, sqlFlags);
        switch (compareType) {
        case Comparison.EQUAL:
            builder.append(" = ");
            break;
        case Comparison.EQUAL_NULL_SAFE:
            builder.append(expression.isNullConstant()
                    || column.getType().getValueType() == Value.BOOLEAN && expression.isConstant() //
                            ? " IS "
                            : " IS NOT DISTINCT FROM ");
            break;
        case Comparison.BIGGER_EQUAL:
            builder.append(" >= ");
            break;
        case Comparison.BIGGER:
            builder.append(" > ");
            break;
        case Comparison.SMALLER_EQUAL:
            builder.append(" <= ");
            break;
        case Comparison.SMALLER:
            builder.append(" < ");
            break;
        case Comparison.IN_LIST:
            Expression.writeExpressions(builder.append(" IN("), expressionList, sqlFlags).append(')');
            break;
        case Comparison.IN_ARRAY:
            return expression.getSQL(builder.append(" = ANY("), sqlFlags, Expression.AUTO_PARENTHESES).append(')');
        case Comparison.IN_QUERY:
            builder.append(" IN(");
            expressionQuery.getPlanSQL(builder, sqlFlags);
            builder.append(')');
            break;
        case Comparison.SPATIAL_INTERSECTS:
            builder.append(" && ");
            break;
        default:
            throw DbException.getInternalError("type=" + compareType);
        }
        if (expression != null) {
            expression.getSQL(builder, sqlFlags, Expression.AUTO_PARENTHESES);
        }
        return builder;
    }

    /**
     * Get the comparison bit mask.
     *
     * @param indexConditions all index conditions
     * @return the mask
     */
    public int getMask(ArrayList<IndexCondition> indexConditions) {
        switch (compareType) {
        case Comparison.FALSE:
            return ALWAYS_FALSE;
        case Comparison.EQUAL:
        case Comparison.EQUAL_NULL_SAFE:
            return EQUALITY;
        case Comparison.IN_LIST:
        case Comparison.IN_ARRAY:
        case Comparison.IN_QUERY:
            if (indexConditions.size() > 1) {
                if (isCompoundColumns()) {
                    Column[] columns = getColumns();
                    for (int i = columns.length; --i >= 0; ) {
                        if (TableType.TABLE != columns[i].getTable().getTableType()) {
                            return 0;
                        }
                    }
                }
                else if (TableType.TABLE != getColumn().getTable().getTableType()) {
                    // if combined with other conditions,
                    // IN(..) can only be used for regular tables
                    // test case:
                    // create table test(a int, b int, primary key(id, name));
                    // create unique index c on test(b, a);
                    // insert into test values(1, 10), (2, 20);
                    // select * from (select * from test)
                    // where a=1 and b in(10, 20);
                    return 0;
                }
            }
            return EQUALITY;
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return START;
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return END;
        case Comparison.SPATIAL_INTERSECTS:
            return SPATIAL_INTERSECTS;
        default:
            throw DbException.getInternalError("type=" + compareType);
        }
    }

    /**
     * Check if the result is always false.
     *
     * @return true if the result will always be false
     */
    public boolean isAlwaysFalse() {
        return compareType == Comparison.FALSE;
    }

    /**
     * Check if this index condition is of the type column larger or equal to
     * value.
     *
     * @return true if this is a start condition
     */
    public boolean isStart() {
        switch (compareType) {
        case Comparison.EQUAL:
        case Comparison.EQUAL_NULL_SAFE:
        case Comparison.BIGGER_EQUAL:
        case Comparison.BIGGER:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if this index condition is of the type column smaller or equal to
     * value.
     *
     * @return true if this is an end condition
     */
    public boolean isEnd() {
        switch (compareType) {
        case Comparison.EQUAL:
        case Comparison.EQUAL_NULL_SAFE:
        case Comparison.SMALLER_EQUAL:
        case Comparison.SMALLER:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check if this index condition is of the type spatial column intersects
     * value.
     *
     * @return true if this is a spatial intersects condition
     */
    public boolean isSpatialIntersects() {
        switch (compareType) {
        case Comparison.SPATIAL_INTERSECTS:
            return true;
        default:
            return false;
        }
    }

    public int getCompareType() {
        return compareType;
    }

    /**
     * Get the referenced column.
     *
     * @return the column
     * @throws DbException if {@link #isCompoundColumns()} is {@code true}
     */
    public Column getColumn() {
        if (!isCompoundColumns()) {
            return column;
        }
        throw DbException.getInternalError("The getColumn() method cannot be with multiple columns.");
    }

    /**
     * Get the referenced columns.
     *
     * @return the column array
     * @throws DbException if {@link #isCompoundColumns()} is {@code false}
     */
    public Column[] getColumns() {
        if (isCompoundColumns()) {
            return columns;
        }
        throw DbException.getInternalError("The getColumns() method cannot be with a single column.");
    }

    /**
     * Check if the expression contains multiple columns
     *
     * @return true if it contains multiple columns
     */
    public boolean isCompoundColumns() {
        return compoundColumns;
    }

    /**
     * Get expression.
     *
     * @return Expression.
     */
    public Expression getExpression() {
        return expression;
    }

    /**
     * Get expression list.
     *
     * @return Expression list.
     */
    public List<Expression> getExpressionList() {
        return expressionList;
    }

    /**
     * Get expression query.
     *
     * @return Expression query.
     */
    public Query getExpressionQuery() {
        return expressionQuery;
    }

    /**
     * Check if the expression can be evaluated.
     *
     * @return true if it can be evaluated
     */
    public boolean isEvaluatable() {
        if (expression != null) {
            return expression
                    .isEverything(ExpressionVisitor.EVALUATABLE_VISITOR);
        }
        if (expressionList != null) {
            for (Expression e : expressionList) {
                if (!e.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    return false;
                }
            }
            return true;
        }
        return expressionQuery
                .isEverything(ExpressionVisitor.EVALUATABLE_VISITOR);
    }

    /**
     * Creates a copy of this index condition but using the
     * {@link Index#getIndexColumns() columns} of the {@code index}.
     *
     * @param index
     *            a non-null Index
     * @return a new IndexCondition with the specified columns, or {@code null}
     *         if the index does not match with this condition.
     */
    public IndexCondition cloneWithIndexColumns(Index index) {
        if (!isCompoundColumns()) {
            throw DbException.getInternalError("The cloneWithColumns() method cannot be with a single column.");
        }

        IndexColumn[] indexColumns = index.getIndexColumns();
        int length = indexColumns.length;
        if (length != columns.length) {
            return null;
        }

        int[] newOrder = new int[length];
        int found = 0;
        for (int i = 0; i < length; i++) {
            if (indexColumns[i] == null || indexColumns[i].column == null) {
                return null;
            }
            for (int j = 0; j < this.columns.length; j++) {
                if (columns[j] == indexColumns[i].column) {
                    newOrder[j] = i;
                    found++;
                }
            }
        }
        if (found != length) {
            return null;
        }

        Column[] newColumns = new Column[length];
        for(int i = 0; i < length; i++) {
            newColumns[i] = columns[newOrder[i]];
        }

        List<Expression> newList = new ArrayList<>(length);
        for (Expression expression: expressionList) {
            if (expression instanceof ValueExpression) {
                ValueExpression valueExpression = (ValueExpression) expression;
                ValueRow currentRow = (ValueRow) valueExpression.getValue(null);
                ValueRow newRow = currentRow.cloneWithOrder(newOrder);
                newList.add(ValueExpression.get(newRow));
            } else if (expression instanceof ExpressionList) {
                ExpressionList currentRow = (ExpressionList) expression;
                ExpressionList newRow = currentRow.cloneWithOrder(newOrder);
                newList.add(newRow);
            }
            else {
                throw DbException.getInternalError("Unexpected expression type: " + expression.getClass());
            }
        }

        return new IndexCondition(Comparison.IN_LIST, null, newColumns, null, newList, null);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (!isCompoundColumns()) {
            builder.append("column=").append(column);
        } else {
            builder.append("columns=");
            Column.writeColumns(builder, columns, TRACE_SQL_FLAGS);
        }
        builder.append(", compareType=");
        return compareTypeToString(builder, compareType)
            .append(", expression=").append(expression)
            .append(", expressionList=").append(expressionList)
            .append(", expressionQuery=").append(expressionQuery).toString();
    }

    private static StringBuilder compareTypeToString(StringBuilder builder, int i) {
        boolean f = false;
        if ((i & EQUALITY) == EQUALITY) {
            f = true;
            builder.append("EQUALITY");
        }
        if ((i & START) == START) {
            if (f) {
                builder.append(", ");
            }
            f = true;
            builder.append("START");
        }
        if ((i & END) == END) {
            if (f) {
                builder.append(", ");
            }
            f = true;
            builder.append("END");
        }
        if ((i & ALWAYS_FALSE) == ALWAYS_FALSE) {
            if (f) {
                builder.append(", ");
            }
            f = true;
            builder.append("ALWAYS_FALSE");
        }
        if ((i & SPATIAL_INTERSECTS) == SPATIAL_INTERSECTS) {
            if (f) {
                builder.append(", ");
            }
            builder.append("SPATIAL_INTERSECTS");
        }
        return builder;
    }

}
