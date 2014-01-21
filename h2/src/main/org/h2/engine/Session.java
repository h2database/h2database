/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

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
import org.h2.constraint.Constraint;
import org.h2.index.Index;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.mvstore.db.MVTable;
import org.h2.mvstore.db.TransactionStore.Change;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.store.DataHandler;
import org.h2.store.InDoubtTransaction;
import org.h2.store.LobStorageFrontend;
import org.h2.table.Table;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public class Session extends SessionWithState {

    /**
     * This special log position means that the log entry has been written.
     */
    public static final int LOG_WRITTEN = -1;

    /**
     * The prefix of generated identifiers. It may not have letters, because
     * they are case sensitive.
     */
    private static final String SYSTEM_IDENTIFIER_PREFIX = "_";
    private static int nextSerialId;

    private final int serialId = nextSerialId++;
    private final Database database;
    private ConnectionInfo connectionInfo;
    private final User user;
    private final int id;
    private final ArrayList<Table> locks = New.arrayList();
    private final UndoLog undoLog;
    private boolean autoCommit = true;
    private Random random;
    private int lockTimeout;
    private Value lastIdentity = ValueLong.get(0);
    private Value lastScopeIdentity = ValueLong.get(0);
    private int firstUncommittedLog = Session.LOG_WRITTEN;
    private int firstUncommittedPos = Session.LOG_WRITTEN;
    private HashMap<String, Savepoint> savepoints;
    private HashMap<String, Table> localTempTables;
    private HashMap<String, Index> localTempTableIndexes;
    private HashMap<String, Constraint> localTempTableConstraints;
    private int throttle;
    private long lastThrottle;
    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private Trace trace;
    private HashMap<String, Value> unlinkLobMap;
    private int systemIdentifier;
    private HashMap<String, Procedure> procedures;
    private boolean undoLogEnabled = true;
    private boolean redoLogBinary = true;
    private boolean autoCommitAtTransactionEnd;
    private String currentTransactionName;
    private volatile long cancelAt;
    private boolean closed;
    private final long sessionStart = System.currentTimeMillis();
    private long transactionStart;
    private long currentCommandStart;
    private HashMap<String, Value> variables;
    private HashSet<ResultInterface> temporaryResults;
    private int queryTimeout;
    private boolean commitOrRollbackDisabled;
    private Table waitForLock;
    private Thread waitForLockThread;
    private int modificationId;
    private int objectId;
    private final int queryCacheSize;
    private SmallLRUCache<String, Command> queryCache;
    private long modificationMetaID = -1;
    private ArrayList<Value> temporaryLobs;

    private Transaction transaction;
    private long startStatement = -1;

    public Session(Database database, User user, int id) {
        this.database = database;
        this.queryTimeout = database.getSettings().maxQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.undoLog = new UndoLog(this);
        this.user = user;
        this.id = id;
        Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_LOCK_TIMEOUT));
        this.lockTimeout = setting == null ? Constants.INITIAL_LOCK_TIMEOUT : setting.getIntValue();
        this.currentSchemaName = Constants.SCHEMA_MAIN;
    }

    public boolean setCommitOrRollbackDisabled(boolean x) {
        boolean old = commitOrRollbackDisabled;
        commitOrRollbackDisabled = x;
        return old;
    }

    private void initVariables() {
        if (variables == null) {
            variables = database.newStringMap();
        }
    }

    /**
     * Set the value of the given variable for this session.
     *
     * @param name the name of the variable (may not be null)
     * @param value the new value (may not be null)
     */
    public void setVariable(String name, Value value) {
        initVariables();
        modificationId++;
        Value old;
        if (value == ValueNull.INSTANCE) {
            old = variables.remove(name);
        } else {
            // link LOB values, to make sure we have our own object
            value = value.link(database, LobStorageFrontend.TABLE_ID_SESSION_VARIABLE);
            old = variables.put(name, value);
        }
        if (old != null) {
            // close the old value (in case it is a lob)
            old.unlink(database);
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
        Value v = variables.get(name);
        return v == null ? ValueNull.INSTANCE : v;
    }

    /**
     * Get the list of variable names that are set for this session.
     *
     * @return the list of names
     */
    public String[] getVariableNames() {
        if (variables == null) {
            return new String[0];
        }
        String[] list = new String[variables.size()];
        variables.keySet().toArray(list);
        return list;
    }

    /**
     * Get the local temporary table if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Table findLocalTempTable(String name) {
        if (localTempTables == null) {
            return null;
        }
        return localTempTables.get(name);
    }

    public ArrayList<Table> getLocalTempTables() {
        if (localTempTables == null) {
            return New.arrayList();
        }
        return New.arrayList(localTempTables.values());
    }

    /**
     * Add a local temporary table to this session.
     *
     * @param table the table to add
     * @throws DbException if a table with this name already exists
     */
    public void addLocalTempTable(Table table) {
        if (localTempTables == null) {
            localTempTables = database.newStringMap();
        }
        if (localTempTables.get(table.getName()) != null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, table.getSQL());
        }
        modificationId++;
        localTempTables.put(table.getName(), table);
    }

    /**
     * Drop and remove the given local temporary table from this session.
     *
     * @param table the table
     */
    public void removeLocalTempTable(Table table) {
        modificationId++;
        localTempTables.remove(table.getName());
        synchronized (database) {
            table.removeChildrenAndResources(this);
        }
    }

    /**
     * Get the local temporary index if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Index findLocalTempTableIndex(String name) {
        if (localTempTableIndexes == null) {
            return null;
        }
        return localTempTableIndexes.get(name);
    }

    public HashMap<String, Index> getLocalTempTableIndexes() {
        if (localTempTableIndexes == null) {
            return New.hashMap();
        }
        return localTempTableIndexes;
    }

    /**
     * Add a local temporary index to this session.
     *
     * @param index the index to add
     * @throws DbException if a index with this name already exists
     */
    public void addLocalTempTableIndex(Index index) {
        if (localTempTableIndexes == null) {
            localTempTableIndexes = database.newStringMap();
        }
        if (localTempTableIndexes.get(index.getName()) != null) {
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, index.getSQL());
        }
        localTempTableIndexes.put(index.getName(), index);
    }

    /**
     * Drop and remove the given local temporary index from this session.
     *
     * @param index the index
     */
    public void removeLocalTempTableIndex(Index index) {
        if (localTempTableIndexes != null) {
            localTempTableIndexes.remove(index.getName());
            synchronized (database) {
                index.removeChildrenAndResources(this);
            }
        }
    }

    /**
     * Get the local temporary constraint if one exists with that name, or
     * null if not.
     *
     * @param name the constraint name
     * @return the constraint, or null
     */
    public Constraint findLocalTempTableConstraint(String name) {
        if (localTempTableConstraints == null) {
            return null;
        }
        return localTempTableConstraints.get(name);
    }

    /**
     * Get the map of constraints for all constraints on local, temporary
     * tables, if any. The map's keys are the constraints' names.
     *
     * @return the map of constraints, or null
     */
    public HashMap<String, Constraint> getLocalTempTableConstraints() {
        if (localTempTableConstraints == null) {
            return New.hashMap();
        }
        return localTempTableConstraints;
    }

    /**
     * Add a local temporary constraint to this session.
     *
     * @param constraint the constraint to add
     * @throws DbException if a constraint with the same name already exists
     */
    public void addLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints == null) {
            localTempTableConstraints = database.newStringMap();
        }
        String name = constraint.getName();
        if (localTempTableConstraints.get(name) != null) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraint.getSQL());
        }
        localTempTableConstraints.put(name, constraint);
    }

    /**
     * Drop and remove the given local temporary constraint from this session.
     *
     * @param constraint the constraint
     */
    void removeLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints != null) {
            localTempTableConstraints.remove(constraint.getName());
            synchronized (database) {
                constraint.removeChildrenAndResources(this);
            }
        }
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    public User getUser() {
        return user;
    }

    @Override
    public void setAutoCommit(boolean b) {
        autoCommit = b;
    }

    public int getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    @Override
    public synchronized CommandInterface prepareCommand(String sql, int fetchSize) {
        return prepareLocal(sql);
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks the
     * rights.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Prepared prepare(String sql) {
        return prepare(sql, false);
    }

    /**
     * Parse and prepare the given SQL statement.
     *
     * @param sql the SQL statement
     * @param rightsChecked true if the rights have already been checked
     * @return the prepared statement
     */
    public Prepared prepare(String sql, boolean rightsChecked) {
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
    public Command prepareLocal(String sql) {
        if (closed) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
        Command command;
        if (queryCacheSize > 0) {
            if (queryCache == null) {
                queryCache = SmallLRUCache.newInstance(queryCacheSize);
                modificationMetaID = database.getModificationMetaId();
            } else {
                long newModificationMetaID = database.getModificationMetaId();
                if (newModificationMetaID != modificationMetaID) {
                    queryCache.clear();
                    modificationMetaID = newModificationMetaID;
                }
                command = queryCache.get(sql);
                if (command != null && command.canReuse()) {
                    command.reuse();
                    return command;
                }
            }
        }
        Parser parser = new Parser(this);
        command = parser.prepareCommand(sql);
        if (queryCache != null) {
            if (command.isCacheable()) {
                queryCache.put(sql, command);
            }
        }
        return command;
    }

    public Database getDatabase() {
        return database;
    }

    @Override
    public int getPowerOffCount() {
        return database.getPowerOffCount();
    }

    @Override
    public void setPowerOffCount(int count) {
        database.setPowerOffCount(count);
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     *
     * @param ddl if the statement was a data definition statement
     */
    public void commit(boolean ddl) {
        checkCommitRollback();
        currentTransactionName = null;
        transactionStart = 0;
        if (transaction != null) {
            // increment the data mod count, so that other sessions
            // see the changes
            // TODO should not rely on locking
            if (locks.size() > 0) {
                for (int i = 0, size = locks.size(); i < size; i++) {
                    Table t = locks.get(i);
                    if (t instanceof MVTable) {
                        ((MVTable) t).commit();
                    }
                }
            }
            transaction.commit();
            transaction = null;
        }
        if (containsUncommitted()) {
            // need to commit even if rollback is not possible
            // (create/drop table and so on)
            database.commit(this);
        }
        if (temporaryLobs != null) {
            for (Value v : temporaryLobs) {
                if (!v.isLinked()) {
                    v.close();
                }
            }
            temporaryLobs.clear();
        }
        if (undoLog.size() > 0) {
            // commit the rows when using MVCC
            if (database.isMultiVersion()) {
                ArrayList<Row> rows = New.arrayList();
                synchronized (database) {
                    while (undoLog.size() > 0) {
                        UndoLogRecord entry = undoLog.getLast();
                        entry.commit();
                        rows.add(entry.getRow());
                        undoLog.removeLast(false);
                    }
                    for (int i = 0, size = rows.size(); i < size; i++) {
                        Row r = rows.get(i);
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
        if (unlinkLobMap != null && unlinkLobMap.size() > 0) {
            // need to flush the transaction log, because we can't unlink lobs if the
            // commit record is not written
            database.flush();
            for (Value v : unlinkLobMap.values()) {
                v.unlink(database);
                v.close();
            }
            unlinkLobMap = null;
        }
        unlockAll();
    }

    private void checkCommitRollback() {
        if (commitOrRollbackDisabled && locks.size() > 0) {
            throw DbException.get(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED);
        }
    }

    /**
     * Fully roll back the current transaction.
     */
    public void rollback() {
        checkCommitRollback();
        currentTransactionName = null;
        boolean needCommit = false;
        if (undoLog.size() > 0) {
            rollbackTo(null, false);
            needCommit = true;
        }
        if (transaction != null) {
            rollbackTo(null, false);
            needCommit = true;
            // rollback stored the undo operations in the transaction
            // committing will end the transaction
            transaction.commit();
            transaction = null;
        }
        if (locks.size() > 0 || needCommit) {
            database.commit(this);
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
     * @param savepoint the savepoint to which should be rolled back
     * @param trimToSize if the list should be trimmed
     */
    public void rollbackTo(Savepoint savepoint, boolean trimToSize) {
        int index = savepoint == null ? 0 : savepoint.logIndex;
        while (undoLog.size() > index) {
            UndoLogRecord entry = undoLog.getLast();
            entry.undo(this);
            undoLog.removeLast(trimToSize);
        }
        if (transaction != null) {
            long savepointId = savepoint == null ? 0 : savepoint.transactionSavepoint;
            HashMap<String, MVTable> tableMap = database.getMvStore().getTables();
            Iterator<Change> it = transaction.getChanges(savepointId);
            while (it.hasNext()) {
                Change c = it.next();
                MVTable t = tableMap.get(c.mapName);
                if (t != null) {
                    long key = ((ValueLong) c.key).getLong();
                    ValueArray value = (ValueArray) c.value;
                    short op;
                    Row row;
                    if (value == null) {
                        op = UndoLogRecord.INSERT;
                        row = t.getRow(this, key);
                    } else {
                        op = UndoLogRecord.DELETE;
                        row = new Row(value.getList(), Row.MEMORY_CALCULATE);
                    }
                    row.setKey(key);
                    UndoLogRecord log = new UndoLogRecord(t, op, row);
                    log.undo(this);
                }
            }
        }
        if (savepoints != null) {
            String[] names = new String[savepoints.size()];
            savepoints.keySet().toArray(names);
            for (String name : names) {
                Savepoint sp = savepoints.get(name);
                int savepointIndex = sp.logIndex;
                if (savepointIndex > index) {
                    savepoints.remove(name);
                }
            }
        }
    }

    @Override
    public boolean hasPendingTransaction() {
        return undoLog.size() > 0;
    }

    /**
     * Create a savepoint to allow rolling back to this state.
     *
     * @return the savepoint
     */
    public Savepoint setSavepoint() {
        Savepoint sp = new Savepoint();
        sp.logIndex = undoLog.size();
        if (database.getMvStore() != null) {
            sp.transactionSavepoint = getStatementSavepoint();
        }
        return sp;
    }

    public int getId() {
        return id;
    }

    @Override
    public void cancel() {
        cancelAt = System.currentTimeMillis();
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                database.checkPowerOff();
                cleanTempTables(true);
                undoLog.clear();
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
                DbException.throwInternalError();
            }
        }
        locks.add(table);
    }

    /**
     * Add an undo log entry to this session.
     *
     * @param table the table
     * @param operation the operation type (see {@link UndoLogRecord})
     * @param row the row
     */
    public void log(Table table, short operation, Row row) {
        if (table.isMVStore()) {
            return;
        }
        if (undoLogEnabled) {
            UndoLogRecord log = new UndoLogRecord(table, operation, row);
            // called _after_ the row was inserted successfully into the table,
            // otherwise rollback will try to rollback a not-inserted row
            if (SysProperties.CHECK) {
                int lockMode = database.getLockMode();
                if (lockMode != Constants.LOCK_MODE_OFF && !database.isMultiVersion()) {
                    String tableType = log.getTable().getTableType();
                    if (locks.indexOf(log.getTable()) < 0
                            && !Table.TABLE_LINK.equals(tableType)
                            && !Table.EXTERNAL_TABLE_ENGINE.equals(tableType)) {
                        DbException.throwInternalError();
                    }
                }
            }
            undoLog.add(log);
        } else {
            if (database.isMultiVersion()) {
                // see also UndoLogRecord.commit
                ArrayList<Index> indexes = table.getIndexes();
                for (int i = 0, size = indexes.size(); i < size; i++) {
                    Index index = indexes.get(i);
                    index.commit(operation, row);
                }
                row.commit();
            }
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
        // locks is modified in the loop
        for (int i = 0; i < locks.size(); i++) {
            Table t = locks.get(i);
            if (!t.isLockedExclusively()) {
                synchronized (database) {
                    t.unlock(this);
                    locks.remove(i);
                }
                i--;
            }
        }
    }

    /**
     * Unlock just this table.
     *
     * @param t the table to unlock
     */
    void unlock(Table t) {
        locks.remove(t);
    }

    private void unlockAll() {
        if (SysProperties.CHECK) {
            if (undoLog.size() > 0) {
                DbException.throwInternalError();
            }
        }
        if (locks.size() > 0) {
            synchronized (database) {
                // don't use the enhanced for loop to save memory
                for (int i = 0, size = locks.size(); i < size; i++) {
                    Table t = locks.get(i);
                    t.unlock(this);
                }
                locks.clear();
            }
        }
        savepoints = null;
        sessionStateChanged = true;
    }

    private void cleanTempTables(boolean closeSession) {
        if (localTempTables != null && localTempTables.size() > 0) {
            synchronized (database) {
                for (Table table : New.arrayList(localTempTables.values())) {
                    if (closeSession || table.getOnCommitDrop()) {
                        modificationId++;
                        table.setModified();
                        localTempTables.remove(table.getName());
                        table.removeChildrenAndResources(this);
                        if (closeSession) {
                            // need to commit, otherwise recovery might
                            // ignore the table removal
                            database.commit(this);
                        }
                    } else if (table.getOnCommitTruncate()) {
                        table.truncate(this);
                    }
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

    @Override
    public Trace getTrace() {
        if (trace != null && !closed) {
            return trace;
        }
        String traceModuleName = Trace.JDBC + "[" + id + "]";
        if (closed) {
            return new TraceSystem(null).getTrace(traceModuleName);
        }
        trace = database.getTrace(traceModuleName);
        return trace;
    }

    public void setLastIdentity(Value last) {
        this.lastIdentity = last;
        this.lastScopeIdentity = last;
    }

    public Value getLastIdentity() {
        return lastIdentity;
    }

    public void setLastScopeIdentity(Value last) {
        this.lastScopeIdentity = last;
    }

    public Value getLastScopeIdentity() {
        return lastScopeIdentity;
    }

    /**
     * Called when a log entry for this session is added. The session keeps
     * track of the first entry in the transaction log that is not yet committed.
     *
     * @param logId the transaction log id
     * @param pos the position of the log entry in the transaction log
     */
    public void addLogPos(int logId, int pos) {
        if (firstUncommittedLog == Session.LOG_WRITTEN) {
            firstUncommittedLog = logId;
            firstUncommittedPos = pos;
        }
    }

    public int getFirstUncommittedLog() {
        return firstUncommittedLog;
    }

    /**
     * This method is called after the transaction log has written the commit
     * entry for this session.
     */
    void setAllCommitted() {
        firstUncommittedLog = Session.LOG_WRITTEN;
        firstUncommittedPos = Session.LOG_WRITTEN;
    }

    /**
     * Whether the session contains any uncommitted changes.
     *
     * @return true if yes
     */
    public boolean containsUncommitted() {
        if (database.getMvStore() != null) {
            return transaction != null;
        }
        return firstUncommittedLog != Session.LOG_WRITTEN;
    }

    /**
     * Create a savepoint that is linked to the current log position.
     *
     * @param name the savepoint name
     */
    public void addSavepoint(String name) {
        if (savepoints == null) {
            savepoints = database.newStringMap();
        }
        Savepoint sp = new Savepoint();
        sp.logIndex = undoLog.size();
        if (database.getMvStore() != null) {
            sp.transactionSavepoint = getStatementSavepoint();
        }
        savepoints.put(name, sp);
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) {
        checkCommitRollback();
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        Savepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        rollbackTo(savepoint, false);
    }

    /**
     * Prepare the given transaction.
     *
     * @param transactionName the name of the transaction
     */
    public void prepareCommit(String transactionName) {
        if (transaction != null) {
            database.prepareCommit(this, transactionName);
        }
        if (containsUncommitted()) {
            // need to commit even if rollback is not possible (create/drop
            // table and so on)
            database.prepareCommit(this, transactionName);
        }
        currentTransactionName = transactionName;
    }

    /**
     * Commit or roll back the given transaction.
     *
     * @param transactionName the name of the transaction
     * @param commit true for commit, false for rollback
     */
    public void setPreparedTransaction(String transactionName, boolean commit) {
        if (currentTransactionName != null && currentTransactionName.equals(transactionName)) {
            if (commit) {
                commit(false);
            } else {
                rollback();
            }
        } else {
            ArrayList<InDoubtTransaction> list = database.getInDoubtTransactions();
            int state = commit ? InDoubtTransaction.COMMIT : InDoubtTransaction.ROLLBACK;
            boolean found = false;
            if (list != null) {
                for (InDoubtTransaction p: list) {
                    if (p.getTransactionName().equals(transactionName)) {
                        p.setState(state);
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                throw DbException.get(ErrorCode.TRANSACTION_NOT_FOUND_1, transactionName);
            }
        }
    }

    @Override
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
        if (currentCommandStart == 0) {
            currentCommandStart = System.currentTimeMillis();
        }
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
            // ignore InterruptedException
        }
    }

    /**
     * Set the current command of this session. This is done just before
     * executing the statement.
     *
     * @param command the command
     */
    public void setCurrentCommand(Command command) {
        this.currentCommand = command;
        if (queryTimeout > 0 && command != null) {
            long now = System.currentTimeMillis();
            currentCommandStart = now;
            cancelAt = now + queryTimeout;
        }
    }

    /**
     * Check if the current transaction is canceled by calling
     * Statement.cancel() or because a session timeout was set and expired.
     *
     * @throws DbException if the transaction is canceled
     */
    public void checkCanceled() {
        throttle();
        if (cancelAt == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if (time >= cancelAt) {
            cancelAt = 0;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    /**
     * Get the cancel time.
     *
     * @return the time or 0 if not set
     */
    public long getCancel() {
        return cancelAt;
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
        modificationId++;
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
    public JdbcConnection createConnection(boolean columnList) {
        String url;
        if (columnList) {
            url = Constants.CONN_URL_COLUMNLIST;
        } else {
            url = Constants.CONN_URL_INTERNAL;
        }
        return new JdbcConnection(this, getUser().getName(), url);
    }

    @Override
    public DataHandler getDataHandler() {
        return database;
    }

    /**
     * Remember that the given LOB value must be un-linked (disconnected from
     * the table) at commit.
     *
     * @param v the value
     */
    public void unlinkAtCommit(Value v) {
        if (SysProperties.CHECK && !v.isLinked()) {
            DbException.throwInternalError();
        }
        if (unlinkLobMap == null) {
            unlinkLobMap = New.hashMap();
        }
        unlinkLobMap.put(v.toString(), v);
    }

    /**
     * Do not unlink this LOB value at commit any longer.
     *
     * @param v the value
     */
    public void unlinkAtCommitStop(Value v) {
        if (unlinkLobMap != null) {
            unlinkLobMap.remove(v.toString());
        }
    }

    /**
     * Get the next system generated identifiers. The identifier returned does
     * not occur within the given SQL statement.
     *
     * @param sql the SQL statement
     * @return the new identifier
     */
    public String getNextSystemIdentifier(String sql) {
        String identifier;
        do {
            identifier = SYSTEM_IDENTIFIER_PREFIX + systemIdentifier++;
        } while (sql.indexOf(identifier) >= 0);
        return identifier;
    }

    /**
     * Add a procedure to this session.
     *
     * @param procedure the procedure to add
     */
    public void addProcedure(Procedure procedure) {
        if (procedures == null) {
            procedures = database.newStringMap();
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
        return procedures.get(name);
    }

    public void setSchemaSearchPath(String[] schemas) {
        modificationId++;
        this.schemaSearchPath = schemas;
    }

    public String[] getSchemaSearchPath() {
        return schemaSearchPath;
    }

    @Override
    public int hashCode() {
        return serialId;
    }

    @Override
    public String toString() {
        return "#" + serialId + " (user: " + user.getName() + ")";
    }

    public void setUndoLogEnabled(boolean b) {
        this.undoLogEnabled = b;
    }

    public void setRedoLogBinary(boolean b) {
        this.redoLogBinary = b;
    }

    public boolean isUndoLogEnabled() {
        return undoLogEnabled;
    }

    /**
     * Begin a transaction.
     */
    public void begin() {
        autoCommitAtTransactionEnd = true;
        autoCommit = false;
    }

    public long getSessionStart() {
        return sessionStart;
    }

    public long getTransactionStart() {
        if (transactionStart == 0) {
            transactionStart = System.currentTimeMillis();
        }
        return transactionStart;
    }

    public Table[] getLocks() {
        // copy the data without synchronizing
        ArrayList<Table> copy = New.arrayList();
        for (int i = 0; i < locks.size(); i++) {
            try {
                copy.add(locks.get(i));
            } catch (Exception e) {
                // ignore
                break;
            }
        }
        Table[] list = new Table[copy.size()];
        copy.toArray(list);
        return list;
    }

    /**
     * Wait if the exclusive mode has been enabled for another session. This
     * method returns as soon as the exclusive mode has been disabled.
     */
    public void waitIfExclusiveModeEnabled() {
        // Even in exclusive mode, we have to let the LOB session proceed, or we will
        // get deadlocks.
        if (database.getLobSession() == this) {
            return;
        }
        while (true) {
            Session exclusive = database.getExclusiveSession();
            if (exclusive == null || exclusive == this) {
                break;
            }
            if (Thread.holdsLock(exclusive)) {
                // if another connection is used within the connection
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
     * files as soon as possible, and free object ids of temporary tables.
     *
     * @param result the temporary result set
     */
    public void addTemporaryResult(ResultInterface result) {
        if (!result.needToClose()) {
            return;
        }
        if (temporaryResults == null) {
            temporaryResults = New.hashSet();
        }
        if (temporaryResults.size() < 100) {
            // reference at most 100 result sets to avoid memory problems
            temporaryResults.add(result);
        }
    }

    private void closeTemporaryResults() {
        if (temporaryResults != null) {
            for (ResultInterface result : temporaryResults) {
                result.close();
            }
            temporaryResults = null;
        }
    }

    public void setQueryTimeout(int queryTimeout) {
        int max = database.getSettings().maxQueryTimeout;
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

    /**
     * Set the table this session is waiting for, and the thread that is waiting.
     *
     * @param waitForLock the table
     * @param waitForLockThread the current thread (the one that is waiting)
     */
    public void setWaitForLock(Table waitForLock, Thread waitForLockThread) {
        this.waitForLock = waitForLock;
        this.waitForLockThread = waitForLockThread;
    }

    public Table getWaitForLock() {
        return waitForLock;
    }

    public Thread getWaitForLockThread() {
        return waitForLockThread;
    }

    public int getModificationId() {
        return modificationId;
    }

    @Override
    public boolean isReconnectNeeded(boolean write) {
        while (true) {
            boolean reconnect = database.isReconnectNeeded();
            if (reconnect) {
                return true;
            }
            if (write) {
                if (database.beforeWriting()) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public void afterWriting() {
        database.afterWriting();
    }

    @Override
    public SessionInterface reconnect(boolean write) {
        readSessionState();
        close();
        Session newSession = Engine.getInstance().createSession(connectionInfo);
        newSession.sessionState = sessionState;
        newSession.recreateSessionState();
        if (write) {
            while (!newSession.database.beforeWriting()) {
                // wait until we are allowed to write
            }
        }
        return newSession;
    }

    public void setConnectionInfo(ConnectionInfo ci) {
        connectionInfo = ci;
    }

    public Value getTransactionId() {
        if (!database.isPersistent()) {
            return ValueNull.INSTANCE;
        }
        if (database.getMvStore() != null) {
            if (transaction == null) {
                return ValueNull.INSTANCE;
            }
            return ValueString.get(Long.toString(getTransaction().getId()));
        }
        if (undoLog.size() == 0) {
            return ValueNull.INSTANCE;
        }
        return ValueString.get(firstUncommittedLog + "-" + firstUncommittedPos + "-" + id);
    }

    /**
     * Get the next object id.
     *
     * @return the next object id
     */
    public int nextObjectId() {
        return objectId++;
    }

    public boolean isRedoLogBinaryEnabled() {
        return redoLogBinary;
    }

    /**
     * Get the transaction to use for this session.
     *
     * @return the transaction
     */
    public Transaction getTransaction() {
        if (transaction == null) {
            if (database.getMvStore().getStore().isClosed()) {
                throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
            }
            transaction = database.getMvStore().getTransactionStore().begin();
            startStatement = -1;
        }
        return transaction;
    }

    public long getStatementSavepoint() {
        if (startStatement == -1) {
            startStatement = getTransaction().setSavepoint();
        }
        return startStatement;
    }

    /**
     * Start a new statement within a transaction.
     */
    public void startStatementWithinTransaction() {
        startStatement = -1;
    }

    /**
     * Mark the statement as completed. This also close all temporary result
     * set, and deletes all temporary files held by the result sets.
     */
    public void endStatement() {
        startStatement = -1;
        closeTemporaryResults();
    }

    @Override
    public void addTemporaryLob(Value v) {
        if (temporaryLobs == null) {
            temporaryLobs = new ArrayList<Value>();
        }
        temporaryLobs.add(v);
    }

    /**
     * Represents a savepoint (a position in a transaction to where one can roll
     * back to).
     */
    public static class Savepoint {

        /**
         * The undo log index.
         */
        int logIndex;

        /**
         * The transaction savepoint id.
         */
        long transactionSavepoint;
    }

}
