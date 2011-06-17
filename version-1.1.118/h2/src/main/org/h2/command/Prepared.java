/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
import org.h2.index.Index;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

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
    protected String sqlStatement;

    /**
     * The position of the head record (used for indexes).
     */
    protected int headPos = Index.EMPTY_HEAD;

    /**
     * The list of parameters.
     */
    protected ObjectArray<Parameter> parameters;

    /**
     * If the query should be prepared before each execution. This is set for
     * queries with LIKE ?, because the query plan depends on the parameter
     * value.
     */
    protected boolean prepareAlways;

    private long modificationMetaId;
    private Command command;
    private int objectId;
    private int currentRowNumber;
    private int rowScanCount;

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
     * Get the meta data modification id of the database when this statement was
     * compiled.
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
    public void setParameterList(ObjectArray<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * Get the parameter list.
     *
     * @return the parameter list
     */
    public ObjectArray<Parameter> getParameters() {
        return parameters;
    }

    /**
     * Check if all parameters have been set.
     *
     * @throws SQLException if any parameter has not been set
     */
    protected void checkParameters() throws SQLException {
        for (int i = 0; parameters != null && i < parameters.size(); i++) {
            Parameter param = parameters.get(i);
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
     *
     * @throws SQLException
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
        this.sqlStatement = sql;
    }

    /**
     * Get the SQL statement.
     *
     * @return the SQL statement
     */
    public String getSQL() {
        return sqlStatement;
    }

    /**
     * Get the object id to use for the database object that is created in this
     * statement. This id is only set when the object is persistent.
     * If not set, this method returns 0.
     *
     * @return the object id or 0 if not set
     */
    protected int getCurrentObjectId() {
        return objectId;
    }

    /**
     * Get the current object id, or get a new id from the database. The object
     * id is used when creating new database object (CREATE statement).
     *
     * @param needFresh if a fresh id is required
     * @param dataFile if the object id is used for the
     * @return the object id
     */
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
     * Check if this statement was canceled.
     *
     * @throws SQLException if it was canceled
     */
    public void checkCanceled() throws SQLException {
        session.checkCanceled();
        Command c = command != null ? command : session.getCurrentCommand();
        if (c != null) {
            c.checkCanceled();
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

    /**
     * Print information about the statement executed if info trace level is
     * enabled.
     *
     * @param startTime when the statement was started
     * @param count the update count
     */
    void trace(long startTime, int count) throws SQLException {
        if (session.getTrace().isInfoEnabled()) {
            long time = System.currentTimeMillis() - startTime;
            String params;
            if (parameters.size() > 0) {
                StatementBuilder buff = new StatementBuilder(" {");
                int i = 0;
                for (Expression e : parameters) {
                    buff.appendExceptFirst(", ");
                    Value v = e.getValue(session);
                    buff.append(++i).append(": ").append(v.getTraceSQL());
                }
                params = buff.append('}').toString();
            } else {
                params = "";
            }
            session.getTrace().infoSQL(sqlStatement, params, count, time);
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
    protected void setCurrentRowNumber(int rowNumber) throws SQLException {
        if ((rowScanCount++ & 127) == 0) {
            checkCanceled();
        }
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
        return sqlStatement;
    }

    /**
     * Get the SQL snippet of the value list.
     *
     * @param values the value list
     * @return the SQL snippet
     */
    protected String getSQL(Value[] values) {
        StatementBuilder buff = new StatementBuilder();
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            if (v != null) {
                buff.append(v.getSQL());
            }
        }
        return buff.toString();
    }

    /**
     * Get the SQL snippet of the expression list.
     *
     * @param list the expression list
     * @return the SQL snippet
     */
    protected String getSQL(Expression[] list) {
        StatementBuilder buff = new StatementBuilder();
        for (Expression e : list) {
            buff.appendExceptFirst(", ");
            if (e != null) {
                buff.append(e.getSQL());
            }
        }
        return buff.toString();
    }

    /**
     * Set the SQL statement of the exception to the given row.
     *
     * @param ex the exception
     * @param rowId the row number
     * @param values the values of the row
     * @return the exception
     */
    protected SQLException setRow(SQLException ex, int rowId, String values) {
        if (ex instanceof JdbcSQLException) {
            JdbcSQLException e = (JdbcSQLException) ex;
            StringBuilder buff = new StringBuilder();
            if (sqlStatement != null) {
                buff.append(sqlStatement);
            }
            buff.append(" -- ");
            if (rowId > 0) {
                buff.append("row #").append(rowId + 1).append(' ');
            }
            buff.append('(').append(values).append(')');
            e.setSQL(buff.toString());
        }
        return ex;
    }

}
