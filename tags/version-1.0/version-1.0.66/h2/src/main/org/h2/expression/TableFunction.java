/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.tools.SimpleResultSet;
import org.h2.util.MathUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * Implementation of the functions TABLE(..) and TABLE_DISTINCT(..).
 */
public class TableFunction extends Expression implements FunctionCall {
    private boolean distinct;
    private Expression[] args;
    private Column[] columnList;

    public Value getValue(Session session) throws SQLException {
        int todoClassIsNotUsed;
        return getTable(session, args, false, distinct);
    }

    public String getSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append(getName());
        buff.append('(');
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columnList[i].getCreateSQL());
            buff.append("=");
            Expression e = args[i];
            buff.append(e.getSQL());
        }
        buff.append(')');
        return buff.toString();
    }

    public ValueResultSet getTable(Session session, Expression[] args, boolean onlyColumnList, boolean distinct) throws SQLException {
        int len = columnList.length;
        Expression[] header = new Expression[len];
        Database db = session.getDatabase();
        for (int i = 0; i < len; i++) {
            Column c = columnList[i];
            ExpressionColumn col = new ExpressionColumn(db, c);
            header[i] = col;
        }
        LocalResult result = new LocalResult(session, header, len);
        if (distinct) {
            result.setDistinct();
        }
        if (!onlyColumnList) {
            Value[][] list = new Value[len][];
            int rowCount = 0;
            for (int i = 0; i < len; i++) {
                Value v = args[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    list[i] = new Value[0];
                } else {
                    ValueArray array = (ValueArray) v.convertTo(Value.ARRAY);
                    Value[] l = array.getList();
                    list[i] = l;
                    rowCount = Math.max(rowCount, l.length);
                }
            }
            for (int row = 0; row < rowCount; row++) {
                Value[] r = new Value[len];
                for (int j = 0; j < len; j++) {
                    Value[] l = list[j];
                    Value v;
                    if (l.length <= row) {
                        v = ValueNull.INSTANCE;
                    } else {
                        Column c = columnList[j];
                        v = l[row];
                        v = v.convertTo(c.getType());
                        v = v.convertPrecision(c.getPrecision());
                        v = v.convertScale(true, c.getScale());
                    }
                    r[j] = v;
                }
                result.addRow(r);
            }
        }
        result.done();
        ValueResultSet vr = ValueResultSet.get(getSimpleResultSet(result, Integer.MAX_VALUE));
        return vr;
    }

    SimpleResultSet getSimpleResultSet(LocalResult rs,  int maxrows) throws SQLException {
        int columnCount = rs.getVisibleColumnCount();
        SimpleResultSet simple = new SimpleResultSet();
        for (int i = 0; i < columnCount; i++) {
            String name = rs.getColumnName(i);
            int sqlType = DataType.convertTypeToSQLType(rs.getColumnType(i));
            int precision = MathUtils.convertLongToInt(rs.getColumnPrecision(i));
            int scale = rs.getColumnScale(i);
            simple.addColumn(name, sqlType, precision, scale);
        }
        rs.reset();
        for (int i = 0; i < maxrows && rs.next(); i++) {
            Object[] list = new Object[columnCount];
            for (int j = 0; j < columnCount; j++) {
                list[j] = rs.currentRow()[j].getObject();
            }
            simple.addRow(list);
        }
        return simple;
    }

    public int getScale() {
        return 0;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            args[i].mapColumns(resolver, level);
        }
    }

    public int getCost() {
        int cost = 3;
        for (int i = 0; i < args.length; i++) {
            cost += args[i].getCost();
        }
        return cost;
    }

    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    public long getPrecision() {
        return 0;
    }

    public int getType() {
        return Value.RESULT_SET;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public Expression optimize(Session session) throws SQLException {
        boolean allConst = true;
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
            if (!e.isConstant()) {
                allConst = false;
            }
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e != null) {
                e.setEvaluatable(tableFilter, value);
            }
        }
    }

    public void updateAggregate(Session session) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e != null) {
                e.updateAggregate(session);
            }
        }
    }

    public boolean canGetRowCount() {
        return true;
    }

    public Expression[] getArgs() {
        return args;
    }

    public String getName() {
        return distinct ? "TABLE_DISTINCT" : "TABLE";
    }

    public int getParameterCount() {
        return args.length;
    }

    public int getRowCount(Session session) throws SQLException {
        int len = columnList.length;
        int rowCount = 0;
        for (int i = 0; i < len; i++) {
            Expression expr = args[i];
            if (expr.isConstant()) {
                Value v = expr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    ValueArray array = (ValueArray) v.convertTo(Value.ARRAY);
                    Value[] l = array.getList();
                    rowCount = Math.max(rowCount, l.length);
                }
            }
        }
        return rowCount;
    }

    public ValueResultSet getValueForColumnList(Session session, Expression[] nullArgs) throws SQLException {
        return getTable(session, args, true, false);
    }

}
