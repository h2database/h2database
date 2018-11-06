/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * Implementation of the functions TABLE(..) and TABLE_DISTINCT(..).
 */
public class TableFunction extends Function {
    private final boolean distinct;
    private final long rowCount;
    private Column[] columnList;

    TableFunction(Database database, FunctionInfo info, long rowCount) {
        super(database, info);
        distinct = info.type == Function.TABLE_DISTINCT;
        this.rowCount = rowCount;
    }

    @Override
    public Value getValue(Session session) {
        return getTable(session, args, false, distinct);
    }

    @Override
    protected void checkParameterCount(int len) {
        if (len < 1) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, getName(), ">0");
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append(getName()).append('(');
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(columnList[i].getCreateSQL()).append('=');
            args[i].getSQL(builder);
        }
        return builder.append(')');
    }


    @Override
    public String getName() {
        return distinct ? "TABLE_DISTINCT" : "TABLE";
    }

    @Override
    public ValueResultSet getValueForColumnList(Session session,
            Expression[] nullArgs) {
        return getTable(session, args, true, false);
    }

    public void setColumns(ArrayList<Column> columns) {
        this.columnList = columns.toArray(new Column[0]);
    }

    private ValueResultSet getTable(Session session, Expression[] argList,
            boolean onlyColumnList, boolean distinctRows) {
        int len = columnList.length;
        Expression[] header = new Expression[len];
        Database db = session.getDatabase();
        for (int i = 0; i < len; i++) {
            Column c = columnList[i];
            ExpressionColumn col = new ExpressionColumn(db, c);
            header[i] = col;
        }
        LocalResult result = db.getResultFactory().create(session, header, len);
        if (distinctRows) {
            result.setDistinct();
        }
        if (!onlyColumnList) {
            Value[][] list = new Value[len][];
            int rows = 0;
            for (int i = 0; i < len; i++) {
                Value v = argList[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    list[i] = new Value[0];
                } else {
                    ValueArray array = (ValueArray) v.convertTo(Value.ARRAY);
                    Value[] l = array.getList();
                    list[i] = l;
                    rows = Math.max(rows, l.length);
                }
            }
            for (int row = 0; row < rows; row++) {
                Value[] r = new Value[len];
                for (int j = 0; j < len; j++) {
                    Value[] l = list[j];
                    Value v;
                    if (l.length <= row) {
                        v = ValueNull.INSTANCE;
                    } else {
                        Column c = columnList[j];
                        v = l[row];
                        v = c.convert(v);
                        v = v.convertPrecision(c.getPrecision(), false);
                        v = v.convertScale(true, c.getScale());
                    }
                    r[j] = v;
                }
                result.addRow(r);
            }
        }
        result.done();
        return ValueResultSet.get(result, Integer.MAX_VALUE);
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        return getExpressionColumns(session, getTable(session, getArgs(), true, false).getResult());
    }

}
