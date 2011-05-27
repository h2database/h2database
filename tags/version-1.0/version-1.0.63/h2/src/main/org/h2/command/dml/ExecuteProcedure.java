/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Procedure;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

/**
 * Represents an EXECUTE statement.
 */
public class ExecuteProcedure extends Prepared {
    
    private ObjectArray expressions = new ObjectArray();
    private Procedure procedure;

    public ExecuteProcedure(Session session) {
        super(session);
    }
    
    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
    }
    
    public void setExpression(int index, Expression expr) throws SQLException {
        expressions.add(index, expr);
    }
    
    private void setParameters() throws SQLException {
        Prepared prepared = procedure.getPrepared();
        ObjectArray params = prepared.getParameters();
        for (int i = 0; params != null && i < params.size() && i < expressions.size(); i++) {
            Expression expr = (Expression) expressions.get(i);
            Parameter p = (Parameter) params.get(i);
            p.setValue(expr.getValue(session));
        }
    }
    
    public boolean isQuery() {
        Prepared prepared = procedure.getPrepared();        
        return prepared.isQuery();
    }
    
    public int update() throws SQLException {
        setParameters();
        Prepared prepared = procedure.getPrepared();        
        return prepared.update();
    }

    public final LocalResult query(int limit) throws SQLException {
        setParameters();
        Prepared prepared = procedure.getPrepared();        
        return prepared.query(limit);
    }

    public boolean isTransactional() {
        return true;
    }

    public LocalResult queryMeta() throws SQLException {
        Prepared prepared = procedure.getPrepared();        
        return prepared.queryMeta();
    }

}
