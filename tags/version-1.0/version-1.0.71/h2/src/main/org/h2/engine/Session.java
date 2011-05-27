/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.jdbc.JdbcConnection;
import org.h2.log.InDoubtTransaction;
import org.h2.log.LogSystem;
import org.h2.log.UndoLog;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.store.DataHandler;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A session represents a database connection. When using the server mode, this
 * object resides on the server side and communicates with a RemoteSession on
 * the client side.
 */
public class Session implements SessionInterface {

    private User user;
    private int id;
    private Database database;
    private ObjectArray locks = new ObjectArray();
    private UndoLog undoLog;
    private boolean autoCommit = true;
    private Random random;
    private LogSystem logSystem;
    private int lockTimeout;
    private Value lastIdentity = ValueLong.get(0);
    private int firstUncommittedLog = LogSystem.LOG_WRITTEN;
    private int firstUncommittedPos = LogSystem.LOG_WRITTEN;
    private HashMap savepoints;
    private Exception stackTrace = new Exception();
    private HashMap localTempTables;
    private int throttle;
    private long lastThrottle;
    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private String traceModuleName;
    private HashMap unlinkMap;
    private int tempViewIndex;
    private HashMap procedures;
    private static int nextSerialId;
    private int serialId = nextSerialId++;
    private boolean undoLogEnabled = true;
    private boolean autoCommitAtTransactionEnd;
    private String currentTransactionName;
    private volatile long cancelAt;
    private boolean closed;
    private boolean rollbackMode;
    private long sessionStart = System.currentTimeMillis();
    private long currentCommandStart;
    private HashMap variables;
    private HashSet temporaryResults;
    private int queryTimeout = SysProperties.getMaxQueryTimeout();
    private int lastUncommittedDelete;
    private boolean commitOrRollbackDisabled;

    public Session() {
    }
    
    public boolean setCommitOrRollbackDisabled(boolean x) {
        boolean old = commitOrRollbackDisabled;
        commitOrRollbackDisabled = x;
        return old;
    }

    private void initVariables() {
        if (variables == null) {
            variables = new HashMap();
        }
    }

    /**
     * Set the value of the given variable for this session.
     * 
     * @param name the name of the variable (may not be null)
     * @param value the new value (may not be null)
     */
    public void setVariable(String name, Value value) throws SQLException {
        initVariables();
        Value old;
        if (value == ValueNull.INSTANCE) {
            old = (Value) variables.remove(name);
        } else {
            if (value instanceof ValueLob) {
                // link it, to make sure we have our own file
                value = value.link(database, ValueLob.TABLE_ID_SESSION);
            }
            old = (Value) variables.put(name, value);
        }
        if (old != null) {
            // close the old value (in case it is a lob)
            old.unlink();
            old.close();
        }
    }

    /**
     * Get the value of the specified user defined variable. This method always
     * returns a value; it returns ValueNull.INSTANCE if the variable doesn't
     * exist.
     * 
     * @param name the variable name
     * @return the value, or NULL
     */
    public Value getVariable(String name) {
        initVariables();
        Value v = (Value) variables.get(name);
        return v == null ? ValueNull.INSTANCE : v;
    }

    /**
     * Get the local temporary table if one exists with that name, or null if
     * not.
     * 
     * @param name the table name
     * @return the table, or null
     */
    public Table findLocalTempTable(String name) {
        Table t = null;
        if (localTempTables != null) {
            t = (Table) localTempTables.get(name);
        }
        return t;
    }

    public ObjectArray getLocalTempTables() {
        if (localTempTables == null) {
            return new ObjectArray();
        }
        ObjectArray list = new ObjectArray(localTempTables.values());
        return list;
    }

    /**
     * Add a local temporary table to this session.
     * 
     * @param table the table to add
     * @throws SQLException if a table with this name already exists
     */
    public void addLocalTempTable(Table table) throws SQLException {
        if (localTempTables == null) {
            localTempTables = new HashMap();
        }
        if (localTempTables.get(table.getName()) != null) {
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, table.getSQL());
        }
        localTempTables.put(table.getName(), table);
    }

    /**
     * Drop and remove the given local temporary table from this session.
     * 
     * @param table the table
     */
    public void removeLocalTempTable(Table table) throws SQLException {
        localTempTables.remove(table.getName());
        table.removeChildrenAndResources(this);
    }

    protected void finalize() {
        if (!SysProperties.runFinalize) {
            return;
        }
        if (!closed) {
            throw Message.getInternalError("not closed", stackTrace);
        }
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public User getUser() {
        return user;
    }

    /**
     * Change the autocommit setting for this session.
     * 
     * @param b the new value
     */
    public void setAutoCommit(boolean b) {
        autoCommit = b;
    }

    public int getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public SessionInterface createSession(ConnectionInfo ci) throws SQLException {
        return Engine.getInstance().getSession(ci);
    }

    Session(Database database, User user, int id) {
        this.database = database;
        this.undoLog = new UndoLog(this);
        this.user = user;
        this.id = id;
        this.logSystem = database.getLog();
        Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_LOCK_TIMEOUT));
        this.lockTimeout = setting == null ? Constants.INITIAL_LOCK_TIMEOUT : setting.getIntValue();
        this.currentSchemaName = Constants.SCHEMA_MAIN;
    }

    public CommandInterface prepareCommand(String sql, int fetchSize) throws SQLException {
        return prepareLocal(sql);
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks the
     * rights.
     * 
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Prepared prepare(String sql) throws SQLException {
        return prepare(sql, false);
    }

    /**
     * Parse and prepare the given SQL statement.
     * 
     * @param sql the SQL statement
     * @param rightsChecked true if the rights have already been checked
     * @return the prepared statement
     */
    public Prepared prepare(String sql, boolean rightsChecked) throws SQLException {
        Parser parser = new Parser(this);
        parser.setRightsChecked(rightsChecked);
        return parser.prepare(sql);
    }

    /**
     * Parse and prepare the given SQL statement.
     * This method also checks if the connection has been closed.
     * 
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Command prepareLocal(String sql) throws SQLException {
        if (closed) {
            throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
        }
        Parser parser = new Parser(this);
        return parser.prepareCommand(sql);
    }

    public Database getDatabase() {
        return database;
    }

    public int getPowerOffCount() {
        return database.getPowerOffCount();
    }

    public void setPowerOffCount(int count) {
        database.setPowerOffCount(count);
    }

    public int getLastUncommittedDelete() {
        return lastUncommittedDelete;
    }

    public void setLastUncommittedDelete(int deleteId) {
        lastUncommittedDelete = deleteId;
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     * 
     * @param ddl if the statement was a data definition statement
     */
    public void commit(boolean ddl) throws SQLException {
        checkCommitRollback();
        lastUncommittedDelete = 0;
        currentTransactionName = null;
        if (containsUncommitted()) {
            // need to commit even if rollback is not possible
            // (create/drop table and so on)
            logSystem.commit(this);
        }
        if (undoLog.size() > 0) {
            if (database.isMultiVersion()) {
                ArrayList rows = new ArrayList();
                synchronized (database) {
                    while (undoLog.size() > 0) {
                        UndoLogRecord entry = undoLog.getAndRemoveLast();
                        entry.commit();
                        rows.add(entry.getRow());
                    }
                    for (int i = 0; i < rows.size(); i++) {
                        Row r = (Row) rows.get(i);
                        r.commit();
                    }
                }
            }
            undoLog.clear();
        }
        if (!ddl) {
            // do not clean the temp tables if the last command was a
            // create/drop
            cleanTempTables(false);
            if (autoCommitAtTransactionEnd) {
                autoCommit = true;
                autoCommitAtTransactionEnd = false;
            }
        }
        if (unlinkMap != null && unlinkMap.size() > 0) {
            // need to flush the log file, because we can't unlink lobs if the
            // commit record is not written
            logSystem.flush();
            Iterator it = unlinkMap.values().iterator();
            while (it.hasNext()) {
                Value v = (Value) it.next();
                v.unlink();
            }
            unlinkMap = null;
        }
        unlockAll();
    }
    
    private void checkCommitRollback() throws SQLException {
        if (commitOrRollbackDisabled && locks.size() > 0) {
            throw Message.getSQLException(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED);
        }
    }

    /**
     * Fully roll back the current transaction.
     */
    public void rollback() throws SQLException {
        checkCommitRollback();
        currentTransactionName = null;
        boolean needCommit = false;
        if (undoLog.size() > 0) {
            rollbackTo(0);
            needCommit = true;
        }
        if (locks.size() > 0 || needCommit) {
            logSystem.commit(this);
        }
        cleanTempTables(false);
        unlockAll();
        if (autoCommitAtTransactionEnd) {
            autoCommit = true;
            autoCommitAtTransactionEnd = false;
        }
    }
    
    /**
     * Partially roll back the current transaction.
     * 
     * @param index the position to which should be rolled back
     */    
    public void rollbackTo(int index) throws SQLException {
        while (undoLog.size() > index) {
            UndoLogRecord entry = undoLog.getAndRemoveLast();
            rollbackMode = true;
            try {
                entry.undo(this);
            } finally {
                rollbackMode = false;
            }
        }
        if (savepoints != null) {
            String[] names = new String[savepoints.size()];
            savepoints.keySet().toArray(names);
            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                Integer id = (Integer) savepoints.get(names[i]);
                if (id.intValue() > index) {
                    savepoints.remove(name);
                }
            }
        }
    }

    public int getLogId() {
        return undoLog.size();
    }

    public int getId() {
        return id;
    }

    public void cancel() {
        cancelAt = System.currentTimeMillis();
    }

    public void close() throws SQLException {
        if (!closed) {
            try {
                cleanTempTables(true);
                database.removeSession(this);
            } finally {
                closed = true;
            }
        }
    }

    /**
     * Add a lock for the given table. The object is unlocked on commit or
     * rollback.
     * 
     * @param table the table that is locked
     */
    public void addLock(Table table) {
        if (SysProperties.CHECK) {
            if (locks.indexOf(table) >= 0) {
                throw Message.getInternalError();
            }
        }
        locks.add(table);
    }

    /**
     * Add an undo log entry to this session.
     * 
     * @param table the table
     * @param type the operation type (see {@link UndoLogRecord})
     * @param row the row
     */
    public void log(Table table, short type, Row row) throws SQLException {
        log(new UndoLogRecord(table, type, row));
    }

    private void log(UndoLogRecord log) throws SQLException {
        // called _after_ the row was inserted successfully into the table,
        // otherwise rollback will try to rollback a not-inserted row
        if (SysProperties.CHECK) {
            int lockMode = database.getLockMode();
            if (lockMode != Constants.LOCK_MODE_OFF && !database.isMultiVersion()) {
                if (locks.indexOf(log.getTable()) < 0 && log.getTable().getTableType() != Table.TABLE_LINK) {
                    throw Message.getInternalError();
                }
            }
        }
        if (undoLogEnabled) {
            undoLog.add(log);
        }
    }

    /**
     * Unlock all read locks. This is done if the transaction isolation mode is
     * READ_COMMITTED.
     */
    public void unlockReadLocks() {
        if (database.isMultiVersion()) {
            // MVCC: keep shared locks (insert / update / delete)
            return;
        }
        for (int i = 0; i < locks.size(); i++) {
            Table t = (Table) locks.get(i);
            if (!t.isLockedExclusively()) {
                synchronized (database) {
                    t.unlock(this);
                    locks.remove(i);
                }
                i--;
            }
        }
    }

    private void unlockAll() throws SQLException {
        if (SysProperties.CHECK) {
            if (undoLog.size() > 0) {
                throw Message.getInternalError();
            }
        }
        synchronized (database) {
            for (int i = 0; i < locks.size(); i++) {
                Table t = (Table) locks.get(i);
                t.unlock(this);
            }
            locks.clear();
        }
        savepoints = null;
    }

    private void cleanTempTables(boolean closeSession) throws SQLException {
        if (localTempTables != null && localTempTables.size() > 0) {
            ObjectArray list = new ObjectArray(localTempTables.values());
            for (int i = 0; i < list.size(); i++) {
                Table table = (Table) list.get(i);
                if (closeSession || table.isOnCommitDrop()) {
                    table.setModified();
                    localTempTables.remove(table.getName());
                    table.removeChildrenAndResources(this);
                } else if (table.isOnCommitTruncate()) {
                    table.truncate(this);
                }
            }
        }
    }

    public Random getRandom() {
        if (random == null) {
            random = new Random();
        }
        return random;
    }

    public Trace getTrace() {
        if (traceModuleName == null) {
            traceModuleName = Trace.JDBC + "[" + id + "]";
        }
        if (closed) {
            return new TraceSystem(null, false).getTrace(traceModuleName);
        }
        return database.getTrace(traceModuleName);
    }

    public void setLastIdentity(Value last) {
        this.lastIdentity = last;
    }

    public Value getLastIdentity() {
        return lastIdentity;
    }

    /**
     * Called when a log entry for this session is added. The session keeps
     * track of the first entry in the log file that is not yet committed.
     * 
     * @param logId the log file id
     * @param pos the position of the log entry in the log file
     */
    public void addLogPos(int logId, int pos) {
        if (firstUncommittedLog == LogSystem.LOG_WRITTEN) {
            firstUncommittedLog = logId;
            firstUncommittedPos = pos;
        }
    }

    public int getFirstUncommittedLog() {
        return firstUncommittedLog;
    }

    public int getFirstUncommittedPos() {
        return firstUncommittedPos;
    }

    public void setAllCommitted() {
        firstUncommittedLog = LogSystem.LOG_WRITTEN;
        firstUncommittedPos = LogSystem.LOG_WRITTEN;
    }

    private boolean containsUncommitted() {
        return firstUncommittedLog != LogSystem.LOG_WRITTEN;
    }

    /**
     * Create a savepoint that is linked to the current log position.
     * 
     * @param name the savepoint name
     */
    public void addSavepoint(String name) {
        if (savepoints == null) {
            savepoints = new HashMap();
        }
        savepoints.put(name, ObjectUtils.getInteger(getLogId()));
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     * 
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) throws SQLException {
        checkCommitRollback();        
        if (savepoints == null) {
            throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        Integer id = (Integer) savepoints.get(name);
        if (id == null) {
            throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        int i = id.intValue();
        rollbackTo(i);
    }

    /**
     * Prepare the given transaction.
     * 
     * @param transactionName the name of the transaction
     */
    public void prepareCommit(String transactionName) throws SQLException {
        if (containsUncommitted()) {
            // need to commit even if rollback is not possible (create/drop
            // table and so on)
            logSystem.prepareCommit(this, transactionName);
        }
        currentTransactionName = transactionName;
    }

    /**
     * Commit or roll back the given transaction.
     * 
     * @param transactionName the name of the transaction
     * @param commit true for commit, false for rollback
     */
    public void setPreparedTransaction(String transactionName, boolean commit) throws SQLException {
        if (currentTransactionName != null && currentTransactionName.equals(transactionName)) {
            if (commit) {
                commit(false);
            } else {
                rollback();
            }
        } else {
            ObjectArray list = logSystem.getInDoubtTransactions();
            int state = commit ? InDoubtTransaction.COMMIT : InDoubtTransaction.ROLLBACK;
            boolean found = false;
            for (int i = 0; list != null && i < list.size(); i++) {
                InDoubtTransaction p = (InDoubtTransaction) list.get(i);
                if (p.getTransaction().equals(transactionName)) {
                    p.setState(state);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw Message.getSQLException(ErrorCode.TRANSACTION_NOT_FOUND_1, transactionName);
            }
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void setThrottle(int throttle) {
        this.throttle = throttle;
    }

    /**
     * Wait for some time if this session is throttled (slowed down).
     */
    public void throttle() {
        if (throttle == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if (lastThrottle + Constants.THROTTLE_DELAY > time) {
            return;
        }
        lastThrottle = time + throttle;
        try {
            Thread.sleep(throttle);
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Set the current command of this session. This is done just before
     * executing the statement.
     * 
     * @param command the command
     * @param startTime the time execution has been started
     */
    public void setCurrentCommand(Command command, long startTime) {
        this.currentCommand = command;
        this.currentCommandStart = startTime;
        if (queryTimeout > 0) {
            cancelAt = startTime + queryTimeout;
        }
    }

    /**
     * Check if the current transaction is cancelled by calling
     * Statement.cancel() or because a session timeout was set and expired.
     * 
     * @throws SQLException if the transaction is cancelled
     */
    public void checkCancelled() throws SQLException {
        throttle();
        if (cancelAt == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if (time >= cancelAt) {
            cancelAt = 0;
            throw Message.getSQLException(ErrorCode.STATEMENT_WAS_CANCELLED);
        }
    }

    public Command getCurrentCommand() {
        return currentCommand;
    }

    public long getCurrentCommandStart() {
        return currentCommandStart;
    }

    public boolean getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(boolean b) {
        this.allowLiterals = b;
    }

    public void setCurrentSchema(Schema schema) {
        this.currentSchemaName = schema.getName();
    }

    public String getCurrentSchemaName() {
        return currentSchemaName;
    }

    /**
     * Create an internal connection. This connection is used when initializing
     * triggers, and when calling user defined functions.
     * 
     * @param columnList if the url should be 'jdbc:columnlist:connection'
     * @return the internal connection
     */
    public JdbcConnection createConnection(boolean columnList) throws SQLException {
        String url;
        if (columnList) {
            url = Constants.CONN_URL_COLUMNLIST;
        } else {
            url = Constants.CONN_URL_INTERNAL;
        }
        return new JdbcConnection(this, getUser().getName(), url);
    }

    public DataHandler getDataHandler() {
        return database;
    }

    /**
     * Remember that the given LOB value must be un-linked (disconnected from
     * the table) at commit.
     * 
     * @param v the value
     */
    public void unlinkAtCommit(ValueLob v) {
        if (SysProperties.CHECK && !v.isLinked()) {
            throw Message.getInternalError();
        }
        if (unlinkMap == null) {
            unlinkMap = new HashMap();
        }
        unlinkMap.put(v.toString(), v);
    }

    /**
     * Do not unlink this LOB value at commit any longer.
     * 
     * @param v the value
     */
    public void unlinkAtCommitStop(Value v) {
        if (unlinkMap != null) {
            unlinkMap.remove(v.toString());
        }
    }

    public String getNextTempViewName() {
        return "TEMP_VIEW_" + tempViewIndex++;
    }

    /**
     * Add a procedure to this session.
     * 
     * @param procedure the procedure to add
     */
    public void addProcedure(Procedure procedure) {
        if (procedures == null) {
            procedures = new HashMap();
        }
        procedures.put(procedure.getName(), procedure);
    }

    /**
     * Remove a procedure from this session.
     * 
     * @param name the name of the procedure to remove
     */
    public void removeProcedure(String name) {
        if (procedures != null) {
            procedures.remove(name);
        }
    }

    /**
     * Get the procedure with the given name, or null
     * if none exists.
     * 
     * @param name the procedure name
     * @return the procedure or null
     */
    public Procedure getProcedure(String name) {
        if (procedures == null) {
            return null;
        }
        return (Procedure) procedures.get(name);
    }

    public void setSchemaSearchPath(String[] schemas) {
        this.schemaSearchPath = schemas;
    }

    public String[] getSchemaSearchPath() {
        return schemaSearchPath;
    }

    public int hashCode() {
        return serialId;
    }

    public void setUndoLogEnabled(boolean b) {
        this.undoLogEnabled = b;
    }

    public boolean getUndoLogEnabled() {
        return undoLogEnabled;
    }

    /**
     * Begin a transaction.
     */
    public void begin() {
        autoCommitAtTransactionEnd = true;
        autoCommit = false;
    }

    public boolean getRollbackMode() {
        return rollbackMode;
    }

    public long getSessionStart() {
        return sessionStart;
    }

    public Table[] getLocks() {
        synchronized (database) {
            Table[] list = new Table[locks.size()];
            locks.toArray(list);
            return list;
        }
    }

    /**
     * Wait if the exclusive mode has been enabled for another session. This
     * method returns as soon as the exclusive mode has been disabled.
     */
    public void waitIfExclusiveModeEnabled() {
        while (true) {
            Session exclusive = database.getExclusiveSession();
            if (exclusive == null || exclusive == this) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Remember the result set and close it as soon as the transaction is
     * committed (if it needs to be closed). This is done to delete temporary
     * files as soon as possible.
     * 
     * @param result the temporary result set
     */
    public void addTemporaryResult(LocalResult result) {
        if (!result.needToClose()) {
            return;
        }
        if (temporaryResults == null) {
            temporaryResults = new HashSet();
        }
        if (temporaryResults.size() < 100) {
            // reference at most 100 result sets to avoid memory problems
            temporaryResults.add(result);
        }
    }

    /**
     * Close all temporary result set. This also deletes all temporary files
     * held by the result sets.
     */
    public void closeTemporaryResults() {
        if (temporaryResults != null) {
            for (Iterator it = temporaryResults.iterator(); it.hasNext();) {
                LocalResult result = (LocalResult) it.next();
                result.close();
            }
            temporaryResults = null;
        }
    }

    public void setQueryTimeout(int queryTimeout) {
        int max = SysProperties.getMaxQueryTimeout();
        if (max != 0 && (max < queryTimeout || queryTimeout == 0)) {
            // the value must be at most max
            queryTimeout = max;
        }
        this.queryTimeout = queryTimeout;
        // must reset the cancel at here,
        // otherwise it is still used
        this.cancelAt = 0;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

}
