/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.ResultSet;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueResultSet;

/**
 * This class represents the statement
 * CALL.
 */
public class Call extends Prepared {


    private Expression expression;
    private Expression[] expressions;

    public Call(Session session) {
        super(session);
    }

    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressions, 1);
        result.done();
        return result;
    }

    public int update() {
        Value v = expression.getValue(session);
        int type = v.getType();
        switch(type) {
        case Value.RESULT_SET:
        case Value.ARRAY:
            // this will throw an exception
            // methods returning a result set may not be called like this.
            return super.update();
        case Value.UNKNOWN:
        case Value.NULL:
            return 0;
        default:
            return v.getInt();
        }
    }

    public ResultInterface query(int maxrows) {
        setCurrentRowNumber(1);
        Value v = expression.getValue(session);
        if (v.getType() == Value.RESULT_SET) {
            ResultSet rs = ((ValueResultSet) v).getResultSet();
            return LocalResult.read(session, rs, maxrows);
        } else if (v.getType() == Value.ARRAY) {
            Value[] list = ((ValueArray) v).getList();
            Expression[] expr = new Expression[list.length];
            for (int i = 0; i < list.length; i++) {
                Value e = list[i];
                Column col = new Column("C" + (i + 1), e.getType(), e.getPrecision(), e.getScale(), e.getDisplaySize());
                expr[i] = new ExpressionColumn(session.getDatabase(), col);
            }
            LocalResult result = new LocalResult(session, expr, list.length);
            result.addRow(list);
            result.done();
            return result;
        }
        LocalResult result = new LocalResult(session, expressions, 1);
        Value[] row = { v };
        result.addRow(row);
        result.done();
        return result;
    }

    public void prepare() {
        expression = expression.optimize(session);
        expressions = new Expression[] { expression };
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public boolean isQuery() {
        return true;
    }

    public boolean isTransactional() {
        return true;
    }

    public boolean isReadOnly() {
        return expression.isEverything(ExpressionVisitor.READONLY);

    }

}
