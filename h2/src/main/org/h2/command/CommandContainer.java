/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

public class CommandContainer extends Command {
    
    private Prepared prepared;
    
    CommandContainer(Parser parser, Prepared prepared) {
        super(parser);
        prepared.setCommand(this);
        this.prepared = prepared;
    }
    
    public ObjectArray getParameters() {
        return prepared.getParameters();
    }
    
    public boolean isTransactional() {
        return prepared.isTransactional();
    }
    
    public boolean isQuery() {
        return prepared.isQuery();
    }
    
    private void recompileIfRequired() throws SQLException {
        if(prepared == null || prepared.needRecompile()) {
            // TODO test with 'always recompile'
            prepared.setModificationId(0);
            String sql = prepared.getSQL();
            ObjectArray oldValues = prepared.getParameters();
            Parser parser = new Parser(session);
            prepared = parser.parseOnly(sql);
            long mod = prepared.getModificationId();
            prepared.setModificationId(0);
            ObjectArray newParams = prepared.getParameters();
            for(int i=0; i<newParams.size(); i++) {
                Value v = ((Expression)oldValues.get(i)).getValue(session);
                Parameter p = (Parameter) newParams.get(i);
                p.setValue(v);
            }            
            prepared.prepare();
            prepared.setModificationId(mod);
        }
    }
    
    public int update() throws SQLException {
        recompileIfRequired();
        // TODO query time: should keep lock time separate from running time 
        start();
        prepared.checkParameters();
        prepared.trace();
        return prepared.update();
    }
    
    public LocalResult query(int maxrows) throws SQLException {
        recompileIfRequired();
        // TODO query time: should keep lock time separate from running time 
        start();
        prepared.checkParameters();
        prepared.trace();
        return prepared.query(maxrows);
    }
    
}
