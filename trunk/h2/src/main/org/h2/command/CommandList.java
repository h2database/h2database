/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

public class CommandList extends Command {

    private Command command;
    private String remaining;
    
    // TODO lock if possible!
    
    public CommandList(Parser parser, Command c, String remaining) {
        super(parser);
        this.command = c;
        this.remaining = remaining;
    }
    
    public ObjectArray getParameters() {
        return command.getParameters();
    }
    
    private void executeRemaining() throws SQLException {
        Command command = session.prepareLocal(remaining);
        if(command.isQuery()) {
            command.query(0);
        } else {
            command.update();
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

}
