/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.util.HasSQL;

/**
 * A prepared statement.
 */
public abstract class Prepared {

    /**
     * The session.
     */
    protected SessionLocal session;

    /**
     * The SQL string.
     */
    protected String sqlStatement;

    /**
     * The SQL tokens.
     */
    protected ArrayList<Token> sqlTokens;

    /**
     * Whether to create a new object (for indexes).
     */
    protected boolean create = true;

    /**
     * The list of parameters.
     */
    protected ArrayList<Parameter> parameters;

    private boolean withParamValues;

    /**
     * If the query should be prepared before each execution. This is set for
     * queries with LIKE ?, because the query plan depends on the parameter
     * value.
     */
    protected boolean prepareAlways;

    private long modificationMetaId;
    private Command command;
    /**
     * Used to preserve object identities on database startup. {@code 0} if
     * object is not stored, {@code -1} if object is stored and its ID is
     * already read, {@code >0} if object is stored and its id is not yet read.
     */
    private int persistedObjectId;
    private long currentRowNumber;
    private int rowScanCount;

    /**
     * Create a new object.
     *
     * @param session the session
     */
    public Prepared(SessionLocal session) {
        this.session = session;
        modificationMetaId = getDatabase().getModificationMetaId();
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
     * @return the result set
     */
    public abstract ResultInterface queryMeta();


    /**
     * Get the command type as defined in CommandInterface
     *
     * @return the statement type
     */
    public abstract int getType();

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
    public boolean needRecompile() {
        Database db = getDatabase();
        if (db == null) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "database closed");
        }
        // parser: currently, compiling every create/drop/... twice
        // because needRecompile return true even for the first execution
        return prepareAlways ||
                modificationMetaId < db.getModificationMetaId() ||
                db.getSettings().recompileAlways;
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
    public void setParameterList(ArrayList<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * Get the parameter list.
     *
     * @return the parameter list
     */
    public ArrayList<Parameter> getParameters() {
        return parameters;
    }

    /**
     * Returns whether values of parameters were specified in SQL.
     *
     * @return are values of parameters were specified in SQL
     */
    public boolean isWithParamValues() {
        return withParamValues;
    }

    /**
     * Sets whether values of parameters were specified in SQL.
     *
     * @param withParamValues
     *            are values of parameters were specified in SQL
     */
    public void setWithParamValues(boolean withParamValues) {
        this.withParamValues = withParamValues;
    }

    /**
     * Check if all parameters have been set.
     *
     * @throws DbException if any parameter has not been set
     */
    protected void checkParameters() {
        if (persistedObjectId < 0) {
            // restore original persistedObjectId on Command re-run
            // i.e. due to concurrent update
            persistedObjectId = ~persistedObjectId;
        }
        if (parameters != null) {
            for (Parameter param : parameters) {
                param.checkSet();
            }
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
    public void prepare() {
        // nothing to do
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    public long update() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the query.
     *
     * @param maxrows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    @SuppressWarnings("unused")
    public ResultInterface query(long maxrows) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Set the SQL statement.
     *
     * @param sql the SQL statement
     * @param sqlTokens the SQL tokens
     */
    public final void setSQL(String sql, ArrayList<Token> sqlTokens) {
        this.sqlStatement = sql;
        this.sqlTokens = sqlTokens;
    }

    /**
     * Get the SQL statement.
     *
     * @return the SQL statement
     */
    public final String getSQL() {
        return sqlStatement;
    }

    /**
     * Get the SQL tokens.
     *
     * @return the SQL tokens
     */
    public final ArrayList<Token> getSQLTokens() {
        return sqlTokens;
    }

    /**
     * Get the object id to use for the database object that is created in this
     * statement. This id is only set when the object is already persisted.
     * If not set, this method returns 0.
     *
     * @return the object id or 0 if not set
     */
    public int getPersistedObjectId() {
        int id = persistedObjectId;
        return id >= 0 ? id : 0;
    }

    /**
     * Get the current object id, or get a new id from the database. The object
     * id is used when creating new database object (CREATE statement). This
     * method may be called only once.
     *
     * @return the object id
     */
    protected int getObjectId() {
        int id = persistedObjectId;
        if (id == 0) {
            id = getDatabase().allocateObjectId();
        } else if (id < 0) {
            throw DbException.getInternalError("Prepared.getObjectId() was called before");
        }
        persistedObjectId = ~persistedObjectId;  // while negative, it can be restored later
        return id;
    }

    /**
     * Get the SQL statement with the execution plan.
     *
     * @param sqlFlags formatting flags
     * @return the execution plan
     */
    public final String getPlanSQL(int sqlFlags) {
        return getPlanSQL(new StringBuilder(), sqlFlags).toString();
    }

    /**
     * Appends the SQL statement with the execution plan.
     *
     * @param builder string builder
     * @param sqlFlags formatting flags
     * @return the execution plan
     */
    public StringBuilder getPlanSQL(StringBuilder builder, int sqlFlags) {
        return builder;
    }

    /**
     * Check if this statement was canceled.
     *
     * @throws DbException if it was canceled
     */
    public void checkCanceled() {
        session.checkCanceled();
        Command c = command != null ? command : session.getCurrentCommand();
        if (c != null) {
            c.checkCanceled();
        }
    }

    /**
     * Set the persisted object id for this statement.
     *
     * @param i the object id
     */
    public void setPersistedObjectId(int i) {
        this.persistedObjectId = i;
        this.create = false;
    }

    /**
     * Set the session for this statement.
     *
     * @param currentSession the new session
     */
    public void setSession(SessionLocal currentSession) {
        this.session = currentSession;
    }

    /**
     * Print information about the statement executed if info trace level is
     * enabled.
     * @param database to update statistics
     * @param startTimeNanos when the statement was started
     * @param rowCount the query or update row count
     */
    void trace(Database database, long startTimeNanos, long rowCount) {
        if (session.getTrace().isInfoEnabled() && startTimeNanos > 0) {
            long deltaTimeNanos = System.nanoTime() - startTimeNanos;
            String params = Trace.formatParams(parameters);
            session.getTrace().infoSQL(sqlStatement, params, rowCount, deltaTimeNanos / 1_000_000L);
        }
        // startTime_nanos can be zero for the command that actually turns on
        // statistics
        if (database != null && database.getQueryStatistics() && startTimeNanos != 0) {
            long deltaTimeNanos = System.nanoTime() - startTimeNanos;
            database.getQueryStatisticsData().update(toString(), deltaTimeNanos, rowCount);
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
    public void setCurrentRowNumber(long rowNumber) {
        if ((++rowScanCount & 127) == 0) {
            checkCanceled();
        }
        this.currentRowNumber = rowNumber;
        setProgress();
    }

    /**
     * Get the current row number.
     *
     * @return the row number
     */
    public long getCurrentRowNumber() {
        return currentRowNumber;
    }

    /**
     * Notifies query progress via the DatabaseEventListener
     */
    private void setProgress() {
        if ((currentRowNumber & 127) == 0) {
            getDatabase().setProgress(DatabaseEventListener.STATE_STATEMENT_PROGRESS, sqlStatement,
                    currentRowNumber, 0L);
        }
    }

    /**
     * Convert the statement to a String.
     *
     * @return the SQL statement
     */
    @Override
    public String toString() {
        return sqlStatement;
    }

    /**
     * Get the SQL snippet of the expression list.
     *
     * @param list the expression list
     * @return the SQL snippet
     */
    public static String getSimpleSQL(Expression[] list) {
        return Expression.writeExpressions(new StringBuilder(), list, HasSQL.TRACE_SQL_FLAGS).toString();
    }

    /**
     * Set the SQL statement of the exception to the given row.
     *
     * @param e the exception
     * @param rowId the row number
     * @param values the values of the row
     * @return the exception
     */
    protected final DbException setRow(DbException e, long rowId, String values) {
        StringBuilder buff = new StringBuilder();
        if (sqlStatement != null) {
            buff.append(sqlStatement);
        }
        buff.append(" -- ");
        if (rowId > 0) {
            buff.append("row #").append(rowId + 1).append(' ');
        }
        buff.append('(').append(values).append(')');
        return e.addSQL(buff.toString());
    }

    public boolean isCacheable() {
        return false;
    }

    public final SessionLocal getSession() {
        return session;
    }

    /**
     * Find and collect all DbObjects, this Prepared depends on.
     *
     * @param dependencies collection of dependencies to populate
     */
    public void collectDependencies(HashSet<DbObject> dependencies) {}

    protected final Database getDatabase() {
        return session.getDatabase();
    }

    /**
     * Returns is this command can be repeated again on locking failure.
     *
     * @return is this command can be repeated again on locking failure
     */
    public boolean isRetryable() {
        return true;
    }

}
