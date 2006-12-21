/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

public abstract class Prepared {
    
    protected String sql;
    protected int headPos = -1;
    protected Session session;
    protected ObjectArray parameters;
    private long modificationId;
    private Command command;
    private int objectId;
    private boolean prepareAlways;
    private int currentRowNumber;

    public boolean needRecompile() throws SQLException {
        Database db = session.getDatabase();
        if(db == null) {
            throw Message.getSQLException(Message.CONNECTION_BROKEN);
        }
        // TODO parser: currently, compiling every create/drop/... twice! because needRecompile return true even for the first execution
        return Constants.RECOMPILE_ALWAYS || prepareAlways || modificationId < db.getModificationMetaId();
    }
    
    public abstract boolean isTransactional();
    
    public boolean isReadOnly() {
        return false;
    }

    public Prepared(Session session) {
        this.session = session;
        modificationId = session.getDatabase().getModificationMetaId();
    }
    
    long getModificationId() {
        return modificationId;
    }
    
    void setModificationId(long id) {
        this.modificationId = id;
    }
    
    public void setParameterList(ObjectArray parameters) {
        this.parameters = parameters;
    }

    public ObjectArray getParameters() {
        return parameters;
    }

    protected void checkParameters() throws SQLException {
        for (int i = 0; parameters != null && i < parameters.size(); i++) {
            Parameter param = (Parameter) parameters.get(i);
            param.checkSet();
        }
    }    
    
    public void setCommand(Command command) {
        this.command = command;
    }
    
    public boolean isQuery() {
        return false;
    }
    
    public void prepare() throws SQLException {
        // nothing to do
    }    

    public int update() throws SQLException {
        throw Message.getSQLException(Message.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    public LocalResult query(int maxrows) throws SQLException {
        throw Message.getSQLException(Message.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }    

    public void setSQL(String sql) {
        this.sql = sql;
    }
    
    public String getSQL() {
        return sql;
    }

    protected int getObjectId(boolean needFresh, boolean dataFile) {
        Database db = session.getDatabase();
        int id = objectId;
        if(id == 0) {
            id = db.allocateObjectId(needFresh, dataFile);
        }
        objectId = 0;
        return id;
    }
    
    public String getPlan() {
        return null;
    }
    
    protected void checkCancelled() throws SQLException {
        // TODO strange code: probably checkCancelled should always be called on the session. fix & test after release 1.0
        if(command != null) {
            command.checkCancelled();
        } else {
            session.checkCancelled();
        }
    }
    
    public void setObjectId(int i) {
        this.objectId = i;
    }

    public void setHeadPos(int headPos) {
        this.headPos = headPos;
    }

    public void setSession(Session currentSession) {
        this.session = currentSession;
    }
    
    void trace() throws SQLException {
        if(session.getTrace().info()) {
            StringBuffer buff = new StringBuffer();
            buff.append(sql);
            if(parameters.size()>0) {
                buff.append(" {");
                for(int i=0; i<parameters.size(); i++) {
                    if(i>0) {
                        buff.append(", ");
                    }
                    buff.append(i+1);
                    buff.append(": ");
                    Expression e = (Expression) parameters.get(i);
                    buff.append(e.getValue(session).getSQL());
                }
                buff.append("};");
            } else {
                buff.append(';');
            }
            session.getTrace().infoSQL(buff.toString());
        }
    }

    public void setPrepareAlways(boolean prepareAlways) {
        this.prepareAlways = prepareAlways;
    }
    
    protected void setCurrentRowNumber(int rowNumber) {
        this.currentRowNumber = rowNumber;
    }

    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

}
