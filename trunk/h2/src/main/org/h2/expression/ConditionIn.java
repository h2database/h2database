/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.dml.Select;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.FunctionTable;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
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
    private final ObjectArray values;
    private Value min, max;
    private int queryLevel;

    public ConditionIn(Database database, Expression left, ObjectArray values) {
        this.database = database;
        this.left = left;
        this.values = values;
    }

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        boolean result = false;
        boolean hasNull = false;
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            Value r = e.getValue(session);
            if (r == ValueNull.INSTANCE) {
                hasNull = true;
            } else {
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
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            e.mapColumns(resolver, queryLevel);
        }
        this.queryLevel = Math.max(queryLevel, this.queryLevel);
    }

    public Expression optimize(Session session) throws SQLException {
        left = left.optimize(session);
        boolean constant = left.isConstant();
        if (constant && left == ValueExpression.NULL) {
            return left;
        }
        boolean allValuesConstant = true;
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            e = e.optimize(session);
            if (allValuesConstant && !e.isConstant()) {
                allValuesConstant = false;
            }
            values.set(i, e);
        }
        if (constant && allValuesConstant) {
            return ValueExpression.get(getValue(session));
        }
        // TODO optimization: could use index in some cases (sort, use min and max)
        if (values.size() == 1) {
            Expression right = (Expression) values.get(0);
            Expression expr = new Comparison(session, Comparison.EQUAL, left, right);
            expr = expr.optimize(session);
            return expr;
        }
        if (SysProperties.OPTIMIZE_IN) {
            int dataType = left.getType();
            ExpressionVisitor independent = ExpressionVisitor.get(ExpressionVisitor.INDEPENDENT);
            independent.queryLevel = queryLevel;
            if (areAllValues(independent)) {
                if (left instanceof ExpressionColumn) {
                    Column column = ((ExpressionColumn) left).getColumn();
                    boolean nullable = column.getNullable();
                    CompareMode mode = session.getDatabase().getCompareMode();
                    for (int i = 0; i < values.size(); i++) {
                        Expression e = (Expression) values.get(i);
                        Value v = e.getValue(session);
                        v = v.convertTo(dataType);
                        values.set(i, ValueExpression.get(v));
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
        if (!SysProperties.OPTIMIZE_IN) {
            return;
        }
        if (min == null && max == null) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        filter.addIndexCondition(new IndexCondition(Comparison.BIGGER_EQUAL, l, ValueExpression.get(min)));
        filter.addIndexCondition(new IndexCondition(Comparison.SMALLER_EQUAL, l, ValueExpression.get(max)));
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            e.setEvaluatable(tableFilter, b);
        }
    }

    public String getSQL() {
        StringBuffer buff = new StringBuffer("(");
        buff.append(left.getSQL());
        buff.append(" IN(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            Expression e = (Expression) values.get(i);
            buff.append(e.getSQL());
        }
        buff.append("))");
        return buff.toString();
    }

    public void updateAggregate(Session session) throws SQLException {
        left.updateAggregate(session);
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
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
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public int getCost() {
        int cost = left.getCost();
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            cost += e.getCost();
        }
        return cost;
    }

    public Expression optimizeInJoin(Session session, Select select) throws SQLException {
        if (!areAllValues(ExpressionVisitor.get(ExpressionVisitor.EVALUATABLE))) {
            return this;
        }
        Database db = session.getDatabase();
        Schema mainSchema = db.getSchema(Constants.SCHEMA_MAIN);
        Function function = Function.getFunction(database, "TABLE_DISTINCT");
        Expression[] array = new Expression[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Expression e = (Expression) values.get(i);
            array[i] = e;
        }
        ExpressionList list = new ExpressionList(array);
        function.setParameter(0, list);
        function.doneWithParameters();
        ObjectArray columns = new ObjectArray();
        int dataType = left.getType();
        String columnName = session.getNextTempViewName() + "_X";
        Column col = new Column(columnName, dataType);
        columns.add(col);
        function.setColumns(columns);
        FunctionTable table = new FunctionTable(mainSchema, session, function);
        String viewName = session.getNextTempViewName();
        TableFilter filter = new TableFilter(session, table, viewName, false, select);
        select.addTableFilter(filter, true);
        ExpressionColumn column = new ExpressionColumn(db, null, viewName, columnName);
        Comparison on = new Comparison(session, Comparison.EQUAL, left, column);
        on.mapColumns(filter, 0);
        filter.addFilterCondition(on, true);
        return ValueExpression.get(ValueBoolean.get(true));
    }

}
