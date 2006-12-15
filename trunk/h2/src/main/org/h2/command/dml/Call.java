/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueResultSet;


/**
 * @author Thomas
 */

public class Call extends Prepared {
    private Expression value;
    private ObjectArray expressions;

    public Call(Session session) {
        super(session);
    }

    public LocalResult query(int maxrows) throws SQLException {
        setCurrentRowNumber(1);
        Value v = value.getValue(session);
        if(v.getType() == Value.RESULT_SET) {
            return LocalResult.read(session, ((ValueResultSet)v).getResultSet());
        } else if(v.getType() == Value.ARRAY) {
            Value[] list = ((ValueArray)v).getList();
            ObjectArray expr = new ObjectArray();
            for(int i = 0; i<list.length; i++) {
                Value e = list[i];
                Column col = new Column("C" + (i+1), e.getType(), e.getPrecision(), e.getScale());
                expr.add(new ExpressionColumn(session.getDatabase(), null, col));
            }
            LocalResult result = new LocalResult(session, expr, list.length);
            result.addRow(list);
            result.done();
            return result;
        }
        LocalResult result = new LocalResult(session, expressions, 1);
        Value[] row = new Value[1];
        row[0] = v;
        result.addRow(row);
        result.done();
        return result;
    }

    public void prepare() throws SQLException {
        value = value.optimize(session);
        expressions = new ObjectArray();
        expressions.add(value);
    }

    public void setValue(Expression expression) {
        value = expression;
    }

    public boolean isQuery() {
        return true;
    }

    public boolean isTransactional() {
        return true;
    }
    
}
