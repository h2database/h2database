/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueString;

/**
 * This class represents the statement
 * EXPLAIN
 */
public class Explain extends Prepared {

    private Prepared command;
    private LocalResult result;
    private boolean executeCommand;

    public Explain(Session session) {
        super(session);
    }

    public void setCommand(Prepared command) {
        this.command = command;
    }

    public void prepare() {
        command.prepare();
    }

    public void setExecuteCommand(boolean executeCommand) {
        this.executeCommand = executeCommand;
    }

    public ResultInterface queryMeta() {
        return query(-1);
    }

    public ResultInterface query(int maxrows) {
        Column column = new Column("PLAN", Value.STRING);
        ExpressionColumn expr = new ExpressionColumn(session.getDatabase(), column);
        Expression[] expressions = { expr };
        result = new LocalResult(session, expressions, 1);
        if (maxrows >= 0) {
            if (executeCommand) {
                if (command.isQuery()) {
                    command.query(maxrows);
                } else {
                    command.update();
                }
            }
            String plan = command.getPlanSQL();
            add(plan);
        }
        result.done();
        return result;
    }

    private void add(String text) {
        Value[] row = { ValueString.get(text) };
        result.addRow(row);
    }

    public boolean isQuery() {
        return true;
    }

    public boolean isTransactional() {
        return true;
    }

    public boolean isReadOnly() {
        return true;
    }
}
