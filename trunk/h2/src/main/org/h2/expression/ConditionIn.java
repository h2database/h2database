/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;
import org.h2.command.dml.Select;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.FunctionTable;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An 'in' condition with a list of values, as in WHERE NAME IN(...)
 */
public class ConditionIn extends Condition {

    private final Database database;
    private Expression left;
    private final ObjectArray<Expression> valueList;
    private Value min, max;
    private int queryLevel;

    /**
     * Create a new IN(..) condition.
     *
     * @param database the database
     * @param left the expression before IN
     * @param values the value list (at least one element)
     */
    public ConditionIn(Database database, Expression left, ObjectArray<Expression> values) {
        this.database = database;
        this.left = left;
        this.valueList = values;
    }

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        boolean result = false;
        boolean hasNull = false;
        for (Expression e : valueList) {
            Value r = e.getValue(session);
            if (r == ValueNull.INSTANCE) {
                hasNull = true;
            } else {
                r = r.convertTo(l.getType());
                result = Comparison.compareNotNull(database, l, r, Comparison.EQUAL);
                if (result) {
                    break;
                }
            }
        }
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(result);
    }

    public void mapColumns(ColumnResolver resolver, int queryLevel) throws SQLException {
        left.mapColumns(resolver, queryLevel);
        for (Expression e : valueList) {
            e.mapColumns(resolver, queryLevel);
        }
        this.queryLevel = Math.max(queryLevel, this.queryLevel);
    }

    public Expression optimize(Session session) throws SQLException {
        left = left.optimize(session);
        boolean constant = left.isConstant();
        if (constant && left == ValueExpression.getNull()) {
            return left;
        }
        boolean allValuesConstant = true;
        for (int i = 0; i < valueList.size(); i++) {
            Expression e = valueList.get(i);
            e = e.optimize(session);
            if (allValuesConstant && !e.isConstant()) {
                allValuesConstant = false;
            }
            valueList.set(i, e);
        }
        if (constant && allValuesConstant) {
            return ValueExpression.get(getValue(session));
        }
        if (valueList.size() == 1) {
            Expression right = valueList.get(0);
            Expression expr = new Comparison(session, Comparison.EQUAL, left, right);
            expr = expr.optimize(session);
            return expr;
        }
        if (SysProperties.OPTIMIZE_IN && !SysProperties.OPTIMIZE_IN_LIST) {
            int dataType = left.getType();
            ExpressionVisitor independent = ExpressionVisitor.get(ExpressionVisitor.INDEPENDENT);
            independent.setQueryLevel(queryLevel);
            if (areAllValues(independent)) {
                if (left instanceof ExpressionColumn) {
                    Column column = ((ExpressionColumn) left).getColumn();
                    boolean nullable = column.isNullable();
                    CompareMode mode = session.getDatabase().getCompareMode();
                    for (int i = 0; i < valueList.size(); i++) {
                        Expression e = valueList.get(i);
                        Value v = e.getValue(session);
                        v = v.convertTo(dataType);
                        valueList.set(i, ValueExpression.get(v));
                        if (min == null || min.compareTo(v, mode) > 0) {
                            if (v != ValueNull.INSTANCE || nullable) {
                                min = v;
                            }
                        }
                        if (max == null || max.compareTo(v, mode) < 0) {
                            max = v;
                        }
                    }
                }
            }
        }
        return this;
    }

    public void createIndexConditions(Session session, TableFilter filter) {
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        if (SysProperties.OPTIMIZE_IN_LIST) {
            ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.NOT_FROM_RESOLVER);
            visitor.setResolver(filter);
            for (Expression e : valueList) {
                if (!e.isEverything(visitor)) {
                    return;
                }
            }
            filter.addIndexCondition(IndexCondition.getInList(l, valueList));
            return;
        }
        if (!SysProperties.OPTIMIZE_IN) {
            return;
        }
        if (min == null && max == null) {
            return;
        }
        filter.addIndexCondition(IndexCondition.get(Comparison.BIGGER_EQUAL, l, ValueExpression.get(min)));
        filter.addIndexCondition(IndexCondition.get(Comparison.SMALLER_EQUAL, l, ValueExpression.get(max)));
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        for (Expression e : valueList) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        buff.append(left.getSQL()).append(" IN(");
        for (Expression e : valueList) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append("))").toString();
    }

    public void updateAggregate(Session session) throws SQLException {
        left.updateAggregate(session);
        for (Expression e : valueList) {
            e.updateAggregate(session);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        if (!left.isEverything(visitor)) {
            return false;
        }
        return areAllValues(visitor);
    }

    private boolean areAllValues(ExpressionVisitor visitor) {
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public int getCost() {
        int cost = left.getCost();
        for (Expression e : valueList) {
            cost += e.getCost();
        }
        return cost;
    }

    public Expression optimizeInJoin(Session session, Select select) throws SQLException {
        if (SysProperties.OPTIMIZE_IN_LIST) {
            return this;
        }
        if (!areAllValues(ExpressionVisitor.get(ExpressionVisitor.EVALUATABLE))) {
            return this;
        }
        if (!areAllValues(ExpressionVisitor.get(ExpressionVisitor.INDEPENDENT))) {
            return this;
        }
        if (!(left instanceof ExpressionColumn)) {
            return this;
        }
        ExpressionColumn ec = (ExpressionColumn) left;
        Index index = ec.getTableFilter().getTable().getIndexForColumn(ec.getColumn(), false);
        if (index == null) {
            return this;
        }
        Database db = session.getDatabase();
        Schema mainSchema = db.getSchema(Constants.SCHEMA_MAIN);
        int rowCount = valueList.size();
        TableFunction function = new TableFunction(database, Function.getFunctionInfo("TABLE_DISTINCT"), rowCount);
        Expression[] array = new Expression[rowCount];
        for (int i = 0; i < rowCount; i++) {
            Expression e = valueList.get(i);
            array[i] = e;
        }
        ExpressionList list = new ExpressionList(array);
        function.setParameter(0, list);
        function.doneWithParameters();
        ObjectArray<Column> columns = ObjectArray.newInstance();
        int dataType = left.getType();
        String columnName = session.getNextSystemIdentifier(select.getSQL());
        Column col = new Column(columnName, dataType);
        columns.add(col);
        function.setColumns(columns);
        FunctionTable table = new FunctionTable(mainSchema, session, function, function);
        String viewName = session.getNextSystemIdentifier(select.getSQL());
        TableFilter filter = new TableFilter(session, table, viewName, false, select);
        select.addTableFilter(filter, true);
        ExpressionColumn column = new ExpressionColumn(db, null, viewName, columnName);
        Expression on = new Comparison(session, Comparison.EQUAL, left, column);
        on.mapColumns(filter, 0);
        on = on.optimize(session);
        return new ConditionAndOr(ConditionAndOr.AND, this, on);
    }

    /**
     * Add an additional element if possible. Example: given two conditions
     * A IN(1, 2) OR A=3, the constant 3 is added: A IN(1, 2, 3).
     *
     * @param session the session
     * @param other the second condition
     * @return null if the condition was not added, or the new condition
     */
    public Expression getAdditional(Session session, Comparison other) {
        Expression add = other.getIfEquals(left);
        if (add != null) {
            valueList.add(add);
            return this;
        }
        return null;
    }

}
