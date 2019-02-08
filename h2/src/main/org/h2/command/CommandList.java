/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;

import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;

/**
 * Represents a list of SQL statements.
 */
class CommandList extends Command {

    private final ArrayList<Command> commands;
    private final ArrayList<Parameter> parameters;

    CommandList(Session session, String sql, ArrayList<Command> commands, ArrayList<Parameter> parameters) {
        super(session, sql);
        this.commands = commands;
        this.parameters = parameters;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return parameters;
    }

    private void executeRemaining() {
        for (int i = 1, l = commands.size(); i < l; i++) {
            Command command = commands.get(i);
            if (command.isQuery()) {
                command.query(0);
            } else {
                command.update();
            }
        }
    }

    @Override
    public int update() {
        int updateCount = commands.get(0).executeUpdate(false).getUpdateCount();
        executeRemaining();
        return updateCount;
    }

    @Override
    public void prepareJoinBatch() {
        commands.get(0).prepareJoinBatch();
    }

    @Override
    public ResultInterface query(int maxrows) {
        ResultInterface result = commands.get(0).query(maxrows);
        executeRemaining();
        return result;
    }

    @Override
    public boolean isQuery() {
        return commands.get(0).isQuery();
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return commands.get(0).queryMeta();
    }

    @Override
    public int getCommandType() {
        return commands.get(0).getCommandType();
    }

}
