/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.expression.ParameterInterface;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

/**
 * Represents a list of SQL statements.
 */
public class CommandList extends Command {

    private final Command command;
    private final String remaining;

    public CommandList(Parser parser, String sql, Command c, String remaining) {
        super(parser, sql);
        this.command = c;
        this.remaining = remaining;
    }

    public ObjectArray< ? extends ParameterInterface> getParameters() {
        return command.getParameters();
    }

    private void executeRemaining() throws SQLException {
        Command remainingCommand = session.prepareLocal(remaining);
        if (remainingCommand.isQuery()) {
            remainingCommand.query(0);
        } else {
            remainingCommand.update();
        }
    }

    public int update() throws SQLException {
        int updateCount = command.executeUpdate();
        executeRemaining();
        return updateCount;
    }

    public LocalResult query(int maxrows) throws SQLException {
        LocalResult result = command.query(maxrows);
        executeRemaining();
        return result;
    }

    public boolean isQuery() {
        return command.isQuery();
    }

    public boolean isTransactional() {
        return true;
    }

    public boolean isReadOnly() {
        return false;
    }

    public LocalResult queryMeta() throws SQLException {
        return command.queryMeta();
    }

}
