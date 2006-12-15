/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueString;

public class ExplainPlan extends Prepared {

    private Prepared command;
    private LocalResult result;
    
    public ExplainPlan(Session session) {
        super(session);
    }
    
    public void setCommand(Prepared command) {
        this.command = command;
    }
    
    public void prepare() throws SQLException {
        command.prepare();
    }

    public LocalResult query(int maxrows) throws SQLException {
        // TODO rights: are rights required for explain?
        ObjectArray expressions = new ObjectArray();
        Column column = new Column("PLAN", Value.STRING, 0, 0);
        ExpressionColumn expr = new ExpressionColumn(session.getDatabase(), null, column);
        expressions.add(expr);
        result = new LocalResult(session, expressions, 1);
        String plan = command.getPlan();
        add(plan);
        result.done();
        return result;
    }
    
    private void add(String text) throws SQLException {
        Value[] row = new Value[1];
        Value value = ValueString.get(text);
        row[0] = value;
        result.addRow(row);
    }
    
    public boolean isQuery() {
        return true;
    }    
    
    public boolean isTransactional() {
        return true;
    }
}
