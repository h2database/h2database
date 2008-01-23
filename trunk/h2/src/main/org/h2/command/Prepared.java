/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

/**
 * A prepared statement.
 */
public abstract class Prepared {

    /**
     * The session.
     */
    protected Session session;

    /**
     * The SQL string.
     */
    protected String sql;

    /**
     * The position of the head record (used for indexes).
     */
    protected int headPos = -1;

    /**
     * The list of parameters.
     */
    protected ObjectArray parameters;

    /**
     * If the query should be prepared before each execution.
     * This is set for queries with LIKE ?, because the query plan depends on the parameter value.
     */
    protected boolean prepareAlways;

    private long modificationMetaId;
    private Command command;
    private int objectId;
    private int currentRowNumber;

    /**
     * Check if this command is transactional.
     * If it is not, then it forces the current transaction to commit.
     *
     * @return true if it is
     */
    public abstract boolean isTransactional();

    /**
     * Get an empty result set containing the meta data.
     *
     * @return an empty result set
     */
    public abstract LocalResult queryMeta() throws SQLException;

    /**
     * Check if this command is read only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return false;
    }

    /**
     * Create a new object.
     *
     * @param session the session
     */
    public Prepared(Session session) {
        this.session = session;
        modificationMetaId = session.getDatabase().getModificationMetaId();
    }

    /**
     * Check if the statement needs to be re-compiled.
     *
     * @return true if it must
     */
    public boolean needRecompile() throws SQLException {
        Database db = session.getDatabase();
        if (db == null) {
            throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
        }
        // TODO parser: currently, compiling every create/drop/... twice!
        // because needRecompile return true even for the first execution
        return SysProperties.RECOMPILE_ALWAYS || prepareAlways || modificationMetaId < db.getModificationMetaId();
    }

    /**
     * Get the meta data modification id of the database when this statement was compiled.
     *
     * @return the meta data modification id
     */
    long getModificationMetaId() {
        return modificationMetaId;
    }

    /**
     * Set the meta data modification id of this statement.
     *
     * @param id the new id
     */
    void setModificationMetaId(long id) {
        this.modificationMetaId = id;
    }

    /**
     * Set the parameter list of this statement.
     *
     * @param parameters the parameter list
     */
    public void setParameterList(ObjectArray parameters) {
        this.parameters = parameters;
    }

    /**
     * Get the parameter list.
     *
     * @return the parameter list
     */
    public ObjectArray getParameters() {
        return parameters;
    }

    protected void checkParameters() throws SQLException {
        for (int i = 0; parameters != null && i < parameters.size(); i++) {
            Parameter param = (Parameter) parameters.get(i);
            param.checkSet();
        }
    }

    /**
     * Set the command.
     *
     * @param command the new command
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * Check if this object is a query.
     *
     * @return true if it is
     */
    public boolean isQuery() {
        return false;
    }

    /**
     * Prepare this statement.
     */
    public void prepare() throws SQLException {
        // nothing to do
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws SQLException if it is a query
     */
    public int update() throws SQLException {
        throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the query.
     *
     * @param maxrows the maximum number of rows to return
     * @return the result set
     * @throws SQLException if it is not a query
     */
    public LocalResult query(int maxrows) throws SQLException {
        throw Message.getSQLException(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Set the SQL statement.
     *
     * @param sql the SQL statement
     */
    public void setSQL(String sql) {
        this.sql = sql;
    }

    /**
     * Get the SQL statement.
     *
     * @return the SQL statement
     */
    public String getSQL() {
        return sql;
    }

    protected int getObjectId(boolean needFresh, boolean dataFile) {
        Database db = session.getDatabase();
        int id = objectId;
        if (id == 0) {
            id = db.allocateObjectId(needFresh, dataFile);
        }
        objectId = 0;
        return id;
    }

    /**
     * Get the SQL statement with the execution plan.
     *
     * @return the execution plan
     */
    public String getPlanSQL() {
        return null;
    }

    /**
     * Check if this statement was cancelled.
     *
     * @throws SQLException if it was cancelled
     */
    public void checkCancelled() throws SQLException {
        // TODO strange code: probably checkCancelled should always be called on the session. fix & test after release 1.0
        if (command != null) {
            command.checkCancelled();
        } else {
            session.checkCancelled();
        }
    }

    /**
     * Set the object id for this statement.
     *
     * @param i the object id
     */
    public void setObjectId(int i) {
        this.objectId = i;
    }

    /**
     * Set the head position.
     *
     * @param headPos the head position
     */
    public void setHeadPos(int headPos) {
        this.headPos = headPos;
    }

    /**
     * Set the session for this statement.
     *
     * @param currentSession the new session
     */
    public void setSession(Session currentSession) {
        this.session = currentSession;
    }

    void trace() throws SQLException {
        if (session.getTrace().info()) {
            StringBuffer buff = new StringBuffer();
            buff.append(sql);
            if (parameters.size() > 0) {
                buff.append(" {");
                for (int i = 0; i < parameters.size(); i++) {
                    if (i > 0) {
                        buff.append(", ");
                    }
                    buff.append(i + 1);
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

    /**
     * Set the prepare always flag.
     * If set, the statement is re-compiled whenever it is executed.
     *
     * @param prepareAlways the new value
     */
    public void setPrepareAlways(boolean prepareAlways) {
        this.prepareAlways = prepareAlways;
    }

    /**
     * Set the current row number.
     *
     * @param rowNumber the row number
     */
    protected void setCurrentRowNumber(int rowNumber) {
        this.currentRowNumber = rowNumber;
    }

    /**
     * Get the current row number.
     *
     * @return the row number
     */
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    /**
     * Convert the statement to a String.
     *
     * @return the SQL statement
     */
    public String toString() {
        return sql;
    }

}
