/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.ParserBase;
import org.h2.command.Prepared;
import org.h2.command.ddl.Analyze;
import org.h2.command.query.Query;
import org.h2.constraint.Constraint;
import org.h2.index.Index;
import org.h2.index.QueryExpressionIndex;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.meta.DatabaseMeta;
import org.h2.jdbc.meta.DatabaseMetaLocal;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.db.MVIndex;
import org.h2.mvstore.db.MVTable;
import org.h2.mvstore.db.Store;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.store.DataHandler;
import org.h2.store.InDoubtTransaction;
import org.h2.store.LobStorageFrontend;
import org.h2.table.Table;
import org.h2.util.DateTimeUtils;
import org.h2.util.HasSQL;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.SmallLRUCache;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueVarchar;
import org.h2.value.VersionedValue;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataInMemory;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public final class SessionLocal extends Session implements TransactionStore.RollbackListener {

    public enum State { INIT, RUNNING, BLOCKED, SLEEP, THROTTLED, SUSPENDED, CLOSED }

    private static final class SequenceAndPrepared {

        private final Sequence sequence;

        private final Prepared prepared;

        SequenceAndPrepared(Sequence sequence, Prepared prepared) {
            this.sequence = sequence;
            this.prepared = prepared;
        }

        @Override
        public int hashCode() {
            return 31 * (31 + prepared.hashCode()) + sequence.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != SequenceAndPrepared.class) {
                return false;
            }
            SequenceAndPrepared other = (SequenceAndPrepared) obj;
            return sequence == other.sequence && prepared == other.prepared;
        }

    }

    private static final class RowNumberAndValue {

        long rowNumber;

        Value nextValue;

        RowNumberAndValue(long rowNumber, Value nextValue) {
            this.rowNumber = rowNumber;
            this.nextValue = nextValue;
        }

    }

    /**
     * The prefix of generated identifiers. It may not have letters, because
     * they are case sensitive.
     */
    private static final String SYSTEM_IDENTIFIER_PREFIX = "_";
    private static int nextSerialId;

    /**
     * Thread local session for comparison operations between different data types.
     */
    private static final ThreadLocal<Session> THREAD_LOCAL_SESSION = new ThreadLocal<>();

    static Session getThreadLocalSession() {
        Session session = THREAD_LOCAL_SESSION.get();
        if (session == null) {
            THREAD_LOCAL_SESSION.remove();
        }
        return session;
    }

    private final int serialId = nextSerialId++;
    private Database database;
    private User user;
    private final int id;

    private NetworkConnectionInfo networkConnectionInfo;

    private final ArrayList<Table> locks = Utils.newSmallArrayList();
    private boolean autoCommit = true;
    private Random random;
    private int lockTimeout;

    private HashMap<SequenceAndPrepared, RowNumberAndValue> nextValueFor;
    private WeakHashMap<Sequence, Value> currentValueFor;
    private Value lastIdentity = ValueNull.INSTANCE;

    private HashMap<String, Savepoint> savepoints;
    private HashMap<String, Table> localTempTables;
    private HashMap<String, Index> localTempTableIndexes;
    private HashMap<String, Constraint> localTempTableConstraints;
    private int throttleMs;
    private long lastThrottleNs;
    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private Trace trace;
    private HashMap<String, ValueLob> removeLobMap;
    private int systemIdentifier;
    private HashMap<String, Procedure> procedures;
    private boolean autoCommitAtTransactionEnd;
    private String currentTransactionName;
    private volatile long cancelAtNs;
    private final ValueTimestampTimeZone sessionStart;
    private Instant commandStartOrEnd;
    private ValueTimestampTimeZone currentTimestamp;
    private HashMap<String, Value> variables;
    private int queryTimeout;
    private boolean commitOrRollbackDisabled;
    private Table waitForLock;
    private Thread waitForLockThread;
    private int modificationId;
    private int objectId;
    private final int queryCacheSize;
    private SmallLRUCache<String, Command> queryCache;
    private long modificationMetaID = -1;
    private int createViewLevel;
    private volatile SmallLRUCache<Object, QueryExpressionIndex> viewIndexCache;
    private HashMap<Object, QueryExpressionIndex> derivedTableIndexCache;
    private boolean lazyQueryExecution;

    private BitSet nonKeywords;

    private TimeZoneProvider timeZone;

    /**
     * Tables marked for ANALYZE after the current transaction is committed.
     * Prevents us calling ANALYZE repeatedly in large transactions.
     */
    private HashSet<Table> tablesToAnalyze;

    /**
     * Temporary LOBs from result sets. Those are kept for some time. The
     * problem is that transactions are committed before the result is returned,
     * and in some cases the next transaction is already started before the
     * result is read (for example when using the server mode, when accessing
     * metadata methods). We can't simply free those values up when starting the
     * next transaction, because they would be removed too early.
     */
    private LinkedList<TimeoutValue> temporaryResultLobs;

    /**
     * The temporary LOBs that need to be removed on commit.
     */
    private ArrayList<ValueLob> temporaryLobs;

    private Transaction transaction;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private long startStatement = -1;

    /**
     * Isolation level.
     */
    private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

    /**
     * The snapshot data modification id. If isolation level doesn't allow
     * non-repeatable reads the session uses a snapshot versions of data. After
     * commit or rollback these snapshots are discarded and cached results of
     * queries may became invalid. Commit and rollback allocate a new data
     * modification id and store it here to forbid usage of older results.
     */
    private long snapshotDataModificationId;

    /**
     * Set of database object ids to be released at the end of transaction
     */
    private BitSet idsToRelease;

    /**
     * Whether length in definitions of data types is truncated.
     */
    private boolean truncateLargeLength;

    /**
     * Whether BINARY is parsed as VARBINARY.
     */
    private boolean variableBinary;

    /**
     * Whether INFORMATION_SCHEMA contains old-style tables.
     */
    private boolean oldInformationSchema;

    /**
     * Whether commands are executed in quirks mode to support scripts from older versions of H2.
     */
    private boolean quirksMode;

    public SessionLocal(Database database, User user, int id) {
        this.database = database;
        this.queryTimeout = database.getSettings().maxQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.user = user;
        this.id = id;
        this.lockTimeout = database.getLockTimeout();
        Schema mainSchema = database.getMainSchema();
        this.currentSchemaName = mainSchema != null ? mainSchema.getName()
                : database.sysIdentifier(Constants.SCHEMA_MAIN);
        timeZone = DateTimeUtils.getTimeZone();
        sessionStart = DateTimeUtils.currentTimestamp(timeZone, commandStartOrEnd = Instant.now());
    }

    public void setLazyQueryExecution(boolean lazyQueryExecution) {
        this.lazyQueryExecution = lazyQueryExecution;
    }

    public boolean isLazyQueryExecution() {
        return lazyQueryExecution;
    }

    /**
     * This method is called before and after parsing of view definition and may
     * be called recursively.
     *
     * @param parsingView
     *            {@code true} if this method is called before parsing of view
     *            definition, {@code false} if it is called after it.
     */
    public void setParsingCreateView(boolean parsingView) {
        createViewLevel += parsingView ? 1 : -1;
    }

    public boolean isParsingCreateView() {
        return createViewLevel != 0;
    }

    @Override
    public ArrayList<String> getClusterServers() {
        return new ArrayList<>();
    }

    public boolean setCommitOrRollbackDisabled(boolean x) {
        boolean old = commitOrRollbackDisabled;
        commitOrRollbackDisabled = x;
        return old;
    }

    private void initVariables() {
        if (variables == null) {
            variables = newStringsMap();
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
            if (value instanceof ValueLob) {
                // link LOB values, to make sure we have our own object
                value = ((ValueLob) value).copy(getDatabase(), LobStorageFrontend.TABLE_ID_SESSION_VARIABLE);
            }
            old = variables.put(name, value);
        }
        if (old instanceof ValueLob) {
            ((ValueLob) old).remove();
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
        return variables.keySet().toArray(new String[0]);
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

    public List<Table> getLocalTempTables() {
        if (localTempTables == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(localTempTables.values());
    }

    /**
     * Add a local temporary table to this session.
     *
     * @param table the table to add
     * @throws DbException if a table with this name already exists
     */
    public void addLocalTempTable(Table table) {
        if (localTempTables == null) {
            localTempTables = newStringsMap();
        }
        if (localTempTables.putIfAbsent(table.getName(), table) != null) {
            StringBuilder builder = new StringBuilder();
            table.getSQL(builder, HasSQL.TRACE_SQL_FLAGS).append(" AS ");
            ParserBase.quoteIdentifier(table.getName(), HasSQL.TRACE_SQL_FLAGS);
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, builder.toString());
        }
        modificationId++;
    }

    /**
     * Drop and remove the given local temporary table from this session.
     *
     * @param table the table
     */
    public void removeLocalTempTable(Table table) {
        if (localTempTables != null && localTempTables.remove(table.getName()) != null) {
            modificationId++;
            Database db = database;
            if (db != null) {
                synchronized (db) {
                    table.removeChildrenAndResources(this);
                }
            }
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
            return new HashMap<>();
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
            localTempTableIndexes = newStringsMap();
        }
        if (localTempTableIndexes.putIfAbsent(index.getName(), index) != null) {
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, index.getTraceSQL());
        }
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
            return new HashMap<>();
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
            localTempTableConstraints = newStringsMap();
        }
        String name = constraint.getName();
        if (localTempTableConstraints.putIfAbsent(name, constraint) != null) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraint.getTraceSQL());
        }
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
        if (hasTransaction()) {
            transaction.setTimeoutMillis(lockTimeout);
        }
    }

    @Override
    public CommandInterface prepareCommand(String sql, int fetchSize) {
        lock();
        try {
            return prepareLocal(sql);
        } finally {
            unlock();
        }
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks the
     * rights.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Prepared prepare(String sql) {
        return prepare(sql, false, false);
    }

    /**
     * Parse and prepare the given SQL statement.
     *
     * @param sql the SQL statement
     * @param rightsChecked true if the rights have already been checked
     * @param literalsChecked true if the sql string has already been checked
     *            for literals (only used if ALLOW_LITERALS NONE is set).
     * @return the prepared statement
     */
    public Prepared prepare(String sql, boolean rightsChecked, boolean literalsChecked) {
        Parser parser = new Parser(this);
        parser.setRightsChecked(rightsChecked);
        parser.setLiteralsChecked(literalsChecked);
        return parser.prepare(sql);
    }

    /**
     * Parse a query and prepare its expressions. Rights and literals must be
     * already checked.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Query prepareQueryExpression(String sql) {
        Parser parser = new Parser(this);
        parser.setRightsChecked(true);
        parser.setLiteralsChecked(true);
        return parser.prepareQueryExpression(sql);
    }


    /**
     * Parse and prepare the given SQL statement.
     * This method also checks if the connection has been closed.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Command prepareLocal(String sql) {
        if (isClosed()) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "session closed");
        }
        Command command;
        if (queryCacheSize > 0) {
            if (queryCache == null) {
                queryCache = SmallLRUCache.newInstance(queryCacheSize);
                modificationMetaID = getDatabase().getModificationMetaId();
            } else {
                long newModificationMetaID = getDatabase().getModificationMetaId();
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
        try {
            command = parser.prepareCommand(sql);
        } finally {
            // we can't reuse indexes of derived tables, so just drop the whole cache
            derivedTableIndexCache = null;
        }
        if (queryCache != null) {
            if (command.isCacheable()) {
                queryCache.put(sql, command);
            }
        }
        return command;
    }

    /**
     * Arranges for the specified database object id to be released
     * at the end of the current transaction.
     * @param id to be scheduled
     */
    void scheduleDatabaseObjectIdForRelease(int id) {
        if (idsToRelease == null) {
            idsToRelease = new BitSet();
        }
        idsToRelease.set(id);
    }

    public Database getDatabase() {
        if (database == null) {
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
        }
        return database;
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     *
     * @param ddl if the statement was a data definition statement
     */
    public void commit(boolean ddl) {
        beforeCommitOrRollback();
        if (hasTransaction()) {
            try {
                markUsedTablesAsUpdated();
                transaction.commit();
                removeTemporaryLobs(true);
                endTransaction();
            } finally {
                transaction = null;
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
            analyzeTables();
        }
    }

    private void markUsedTablesAsUpdated() {
        // TODO should not rely on locking
        if (!locks.isEmpty()) {
            for (Table t : locks) {
                if (t instanceof MVTable) {
                    ((MVTable) t).commit();
                }
            }
        }
    }

    private void analyzeTables() {
        // On rare occasions it can be called concurrently (i.e. from close())
        // without proper locking, but instead of oversynchronizing
        // we just skip this optional operation in such case
        if (tablesToAnalyze != null && isLockedByCurrentThread()) {
            // take a local copy and clear because in rare cases we can call
            // back into markTableForAnalyze while iterating here
            HashSet<Table> tablesToAnalyzeLocal = tablesToAnalyze;
            tablesToAnalyze = null;
            int rowCount = getDatabase().getSettings().analyzeSample / 10;
            for (Table table : tablesToAnalyzeLocal) {
                Analyze.analyzeTable(this, table, rowCount, false);
            }
            // analyze can lock the meta
            getDatabase().unlockMeta(this);
            // table analysis opens a new transaction(s),
            // so we need to commit afterwards whatever leftovers might be
            commit(true);
        }
    }

    private void removeTemporaryLobs(boolean onTimeout) {
        if (temporaryLobs != null) {
            for (ValueLob v : temporaryLobs) {
                if (!v.isLinkedToTable()) {
                    v.remove();
                }
            }
            temporaryLobs.clear();
        }
        if (temporaryResultLobs != null && !temporaryResultLobs.isEmpty()) {
            long keepYoungerThan = System.nanoTime() - getDatabase().getSettings().lobTimeout * 1_000_000L;
            while (!temporaryResultLobs.isEmpty()) {
                TimeoutValue tv = temporaryResultLobs.getFirst();
                if (onTimeout && tv.created - keepYoungerThan >= 0) {
                    break;
                }
                ValueLob v = temporaryResultLobs.removeFirst().value;
                if (!v.isLinkedToTable()) {
                    v.remove();
                }
            }
        }
    }

    private void beforeCommitOrRollback() {
        if (commitOrRollbackDisabled && !locks.isEmpty()) {
            throw DbException.get(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED);
        }
        currentTransactionName = null;
        currentTimestamp = null;
        getDatabase().throwLastBackgroundException();
    }

    private void endTransaction() {
        if (removeLobMap != null && !removeLobMap.isEmpty()) {
            for (ValueLob v : removeLobMap.values()) {
                v.remove();
            }
            removeLobMap = null;
        }
        unlockAll();
        if (idsToRelease != null) {
            getDatabase().releaseDatabaseObjectIds(idsToRelease);
            idsToRelease = null;
        }
        if (hasTransaction() && !transaction.allowNonRepeatableRead()) {
            snapshotDataModificationId = getDatabase().getNextModificationDataId();
        }
    }

    /**
     * Returns the data modification id of transaction's snapshot, or 0 if
     * isolation level doesn't use snapshots.
     *
     * @return the data modification id of transaction's snapshot, or 0
     */
    public long getSnapshotDataModificationId() {
        return snapshotDataModificationId;
    }

    /**
     * Fully roll back the current transaction.
     */
    public void rollback() {
        beforeCommitOrRollback();
        if (hasTransaction()) {
            rollbackTo(null);
        }
        idsToRelease = null;
        cleanTempTables(false);
        if (autoCommitAtTransactionEnd) {
            autoCommit = true;
            autoCommitAtTransactionEnd = false;
        }
        endTransaction();
    }

    /**
     * Partially roll back the current transaction.
     *
     * @param savepoint the savepoint to which should be rolled back
     */
    public void rollbackTo(Savepoint savepoint) {
        int index = savepoint == null ? 0 : savepoint.logIndex;
        if (hasTransaction()) {
            markUsedTablesAsUpdated();
            if (savepoint == null) {
                transaction.rollback();
                transaction = null;
            } else {
                transaction.rollbackToSavepoint(savepoint.transactionSavepoint);
            }
        }
        if (savepoints != null) {
            String[] names = savepoints.keySet().toArray(new String[0]);
            for (String name : names) {
                Savepoint sp = savepoints.get(name);
                int savepointIndex = sp.logIndex;
                if (savepointIndex > index) {
                    savepoints.remove(name);
                }
            }
        }

        // Because cache may have captured query result (in Query.lastResult),
        // which is based on data from uncommitted transaction.,
        // It is not valid after rollback, therefore cache has to be cleared.
        if (queryCache != null) {
            queryCache.clear();
        }
    }

    @Override
    public boolean hasPendingTransaction() {
        return hasTransaction() && transaction.hasChanges() && transaction.getStatus() != Transaction.STATUS_PREPARED;
    }

    /**
     * Create a savepoint to allow rolling back to this state.
     *
     * @return the savepoint
     */
    public Savepoint setSavepoint() {
        Savepoint sp = new Savepoint();
        sp.transactionSavepoint = getStatementSavepoint();
        return sp;
    }

    public int getId() {
        return id;
    }

    @Override
    public void cancel() {
        cancelAtNs = Utils.currentNanoTime();
    }

    /**
     * Cancel the transaction and close the session if needed.
     */
    void suspend() {
        cancel();
        if (transitionToState(State.SUSPENDED, false) == State.SLEEP) {
            close();
        }
    }

    @Override
    public void close() {
        // this is the only operation that can be invoked concurrently
        // so, we should prevent double-closure
        if (state.getAndSet(State.CLOSED) != State.CLOSED) {
            try {
                if (queryCache != null) {
                    queryCache.clear();
                }
                database.throwLastBackgroundException();

                database.checkPowerOff();

                // release any open table locks
                if (hasPreparedTransaction()) {
                    removeLobMap = null;
                    endTransaction();
                } else {
                    rollback();
                    removeTemporaryLobs(false);
                    cleanTempTables(true);
                    commit(true);       // temp table removal may have opened new transaction
                }

                // Table#removeChildrenAndResources can take the meta lock,
                // and we need to unlock before we call removeSession(), which might
                // want to take the meta lock using the system session.
                database.unlockMeta(this);
            } finally {
                database.removeSession(this);
                database = null;
                user = null;
            }
        }
    }

    /**
     * Register table as locked within current transaction.
     * Table is unlocked on commit or rollback.
     * It also assumes that table will be modified by transaction.
     *
     * @param table the table that is locked
     */
    public void registerTableAsLocked(Table table) {
        if (SysProperties.CHECK) {
            if (locks.contains(table)) {
                throw DbException.getInternalError(table.toString());
            }
        }
        locks.add(table);
    }

    /**
     * Register table as updated within current transaction.
     * This is used instead of table locking when lock mode is LOCK_MODE_OFF.
     *
     * @param table to register
     */
    public void registerTableAsUpdated(Table table) {
        if (!locks.contains(table)) {
            locks.add(table);
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


    private boolean hasTransaction() {
        return transaction != null;
    }

    private void unlockAll() {
        if (!locks.isEmpty()) {
            Table[] array = locks.toArray(new Table[0]);
            for (Table t : array) {
                if (t != null) {
                    t.unlock(this);
                }
            }
            locks.clear();
        }
        Database.unlockMetaDebug(this);
        savepoints = null;
        sessionStateChanged = true;
    }

    private void cleanTempTables(boolean closeSession) {
        if (localTempTables != null && !localTempTables.isEmpty()) {
            Iterator<Table> it = localTempTables.values().iterator();
            while (it.hasNext()) {
                Table table = it.next();
                if (closeSession || table.getOnCommitDrop()) {
                    modificationId++;
                    table.setModified();
                    it.remove();
                    // Exception thrown in org.h2.engine.Database.removeMeta
                    // if line below is missing with TestDeadlock
                    database.lockMeta(this);
                    table.removeChildrenAndResources(this);
                    if (closeSession) {
                        database.throwLastBackgroundException();
                    }
                } else if (table.getOnCommitTruncate()) {
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

    @Override
    public Trace getTrace() {
        if (trace != null && !isClosed()) {
            return trace;
        }
        String traceModuleName = "jdbc[" + id + "]";
        Database db = database;
        if (isClosed() || db == null) {
            return new TraceSystem(null).getTrace(traceModuleName);
        }
        trace = db.getTraceSystem().getTrace(traceModuleName);
        return trace;
    }

    /**
     * Returns the next value of the sequence in this session.
     *
     * @param sequence
     *            the sequence
     * @param prepared
     *            current prepared command, select, or {@code null}
     * @return the next value of the sequence in this session
     */
    public Value getNextValueFor(Sequence sequence, Prepared prepared) {
        Value value;
        Mode mode = getMode();
        if (mode.nextValueReturnsDifferentValues || prepared == null) {
            value = sequence.getNext(this);
        } else {
            if (nextValueFor == null) {
                nextValueFor = new HashMap<>();
            }
            SequenceAndPrepared key = new SequenceAndPrepared(sequence, prepared);
            RowNumberAndValue data = nextValueFor.get(key);
            long rowNumber = prepared.getCurrentRowNumber();
            if (data != null) {
                if (data.rowNumber == rowNumber) {
                    value = data.nextValue;
                } else {
                    data.nextValue = value = sequence.getNext(this);
                    data.rowNumber = rowNumber;
                }
            } else {
                value = sequence.getNext(this);
                nextValueFor.put(key, new RowNumberAndValue(rowNumber, value));
            }
        }
        WeakHashMap<Sequence, Value> currentValueFor = this.currentValueFor;
        if (currentValueFor == null) {
            this.currentValueFor = currentValueFor = new WeakHashMap<>();
        }
        currentValueFor.put(sequence, value);
        if (mode.takeGeneratedSequenceValue) {
            lastIdentity = value;
        }
        return value;
    }

    /**
     * Returns the current value of the sequence in this session.
     *
     * @param sequence
     *            the sequence
     * @return the current value of the sequence in this session
     * @throws DbException
     *             if current value is not defined
     */
    public Value getCurrentValueFor(Sequence sequence) {
        WeakHashMap<Sequence, Value> currentValueFor = this.currentValueFor;
        if (currentValueFor != null) {
            Value value = currentValueFor.get(sequence);
            if (value != null) {
                return value;
            }
        }
        throw DbException.get(ErrorCode.CURRENT_SEQUENCE_VALUE_IS_NOT_DEFINED_IN_SESSION_1, sequence.getTraceSQL());
    }

    public void setLastIdentity(Value last) {
        this.lastIdentity = last;
    }

    public Value getLastIdentity() {
        return lastIdentity;
    }

    /**
     * Whether the session contains any uncommitted changes.
     *
     * @return true if yes
     */
    public boolean containsUncommitted() {
        return transaction != null && transaction.hasChanges();
    }

    /**
     * Create a savepoint that is linked to the current log position.
     *
     * @param name the savepoint name
     */
    public void addSavepoint(String name) {
        if (savepoints == null) {
            savepoints = newStringsMap();
        }
        savepoints.put(name, setSavepoint());
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) {
        beforeCommitOrRollback();
        Savepoint savepoint;
        if (savepoints == null || (savepoint = savepoints.get(name)) == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        rollbackTo(savepoint);
    }

    /**
     * Prepare the given transaction.
     *
     * @param transactionName the name of the transaction
     */
    public void prepareCommit(String transactionName) {
        if (hasPendingTransaction()) {
            // need to commit even if rollback is not possible (create/drop
            // table and so on)
            getDatabase().prepareCommit(this, transactionName);
        }
        currentTransactionName = transactionName;
    }

    /**
     * Checks presence of prepared transaction in this session.
     *
     * @return {@code true} if there is a prepared transaction,
     *         {@code false} otherwise
     */
    public boolean hasPreparedTransaction() {
        return currentTransactionName != null;
    }

    /**
     * Commit or roll back the given transaction.
     *
     * @param transactionName the name of the transaction
     * @param commit true for commit, false for rollback
     */
    public void setPreparedTransaction(String transactionName, boolean commit) {
        if (hasPreparedTransaction() && currentTransactionName.equals(transactionName)) {
            if (commit) {
                commit(false);
            } else {
                rollback();
            }
        } else {
            ArrayList<InDoubtTransaction> list = getDatabase().getInDoubtTransactions();
            int state = commit ? InDoubtTransaction.COMMIT : InDoubtTransaction.ROLLBACK;
            boolean found = false;
            for (InDoubtTransaction p: list) {
                if (p.getTransactionName().equals(transactionName)) {
                    p.setState(state);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw DbException.get(ErrorCode.TRANSACTION_NOT_FOUND_1,
                        transactionName);
            }
        }
    }

    @Override
    public boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    public boolean isOpen() {
        State current = state.get();
        checkSuspended(current);
        return current != State.CLOSED;
    }

    public void setThrottle(int throttle) {
        this.throttleMs = throttle;
    }

    /**
     * Wait for some time if this session is throttled (slowed down).
     */
    public void throttle() {
        if (throttleMs == 0) {
            return;
        }
        long time = System.nanoTime();
        if (lastThrottleNs != 0L && time - lastThrottleNs < Constants.THROTTLE_DELAY * 1_000_000L) {
            return;
        }
        lastThrottleNs = Utils.nanoTimePlusMillis(time, throttleMs);
        State prevState = transitionToState(State.THROTTLED, false);
        try {
            Thread.sleep(throttleMs);
        } catch (InterruptedException ignore) {
        } finally {
            transitionToState(prevState, false);
        }
    }

    /**
     * Set the current command of this session. This is done just before
     * executing the statement.
     *
     * @param command the command
     */
    private void setCurrentCommand(Command command) {
        State targetState = command == null ? State.SLEEP : State.RUNNING;
        transitionToState(targetState, true);
        if (isOpen()) {
            currentCommand = command;
            commandStartOrEnd = Instant.now();
            if (command != null) {
                if (queryTimeout > 0) {
                    cancelAtNs = Utils.currentNanoTimePlusMillis(queryTimeout);
                }
            } else {
                if (currentTimestamp != null && !getMode().dateTimeValueWithinTransaction) {
                    currentTimestamp = null;
                }
                if (nextValueFor != null) {
                    nextValueFor.clear();
                }
            }
        }
    }

    private State transitionToState(State targetState, boolean checkSuspended) {
        State currentState;
        while((currentState = state.get()) != State.CLOSED &&
                (!checkSuspended || checkSuspended(currentState)) &&
                !state.compareAndSet(currentState, targetState)) {/**/}
        return currentState;
    }

    private boolean checkSuspended(State currentState) {
        if (currentState == State.SUSPENDED) {
            close();
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        return true;
    }

    /**
     * Check if the current transaction is canceled by calling
     * Statement.cancel() or because a session timeout was set and expired.
     *
     * @throws DbException if the transaction is canceled
     */
    public void checkCanceled() {
        throttle();
        long cancel = cancelAtNs;
        if (cancel == 0L) {
            return;
        }
        if (System.nanoTime() - cancel >= 0L) {
            cancelAtNs = 0L;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    /**
     * Get the cancel time.
     *
     * @return the time or 0 if not set
     */
    public long getCancel() {
        return cancelAtNs;
    }

    public Command getCurrentCommand() {
        return currentCommand;
    }

    public ValueTimestampTimeZone getCommandStartOrEnd() {
        return DateTimeUtils.currentTimestamp(timeZone, commandStartOrEnd);
    }

    public boolean getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(boolean b) {
        this.allowLiterals = b;
    }

    public void setCurrentSchema(Schema schema) {
        modificationId++;
        if (queryCache != null) {
            queryCache.clear();
        }
        this.currentSchemaName = schema.getName();
    }

    @Override
    public String getCurrentSchemaName() {
        return currentSchemaName;
    }

    @Override
    public void setCurrentSchemaName(String schemaName) {
        Schema schema = getDatabase().getSchema(schemaName);
        setCurrentSchema(schema);
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
        return getDatabase();
    }

    /**
     * Remember that the given LOB value must be removed at commit.
     *
     * @param v the value
     */
    public void removeAtCommit(ValueLob v) {
        if (v.isLinkedToTable()) {
            if (removeLobMap == null) {
                removeLobMap = new HashMap<>();
            }
            removeLobMap.put(v.toString(), v);
        }
    }

    /**
     * Do not remove this LOB value at commit any longer.
     *
     * @param v the value
     */
    public void removeAtCommitStop(ValueLob v) {
        if (v.isLinkedToTable() && removeLobMap != null) {
            removeLobMap.remove(v.toString());
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
        } while (sql.contains(identifier));
        return identifier;
    }

    /**
     * Add a procedure to this session.
     *
     * @param procedure the procedure to add
     */
    public void addProcedure(Procedure procedure) {
        if (procedures == null) {
            procedures = newStringsMap();
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
        return "#" + serialId + " (user: " + (user == null ? "<null>" : user.getName()) + ", " + state.get() + ")";
    }

    /**
     * Begin a transaction.
     */
    public void begin() {
        autoCommitAtTransactionEnd = true;
        autoCommit = false;
    }

    public ValueTimestampTimeZone getSessionStart() {
        return sessionStart;
    }

    public Set<Table> getLocks() {
        /*
         * This implementation needs to be lock-free.
         */
        if (getDatabase().getLockMode() == Constants.LOCK_MODE_OFF || locks.isEmpty()) {
            return Collections.emptySet();
        }
        /*
         * Do not use ArrayList.toArray(T[]) here, its implementation is not
         * thread-safe.
         */
        Object[] array = locks.toArray();
        /*
         * The returned array may contain null elements and may contain
         * duplicates due to concurrent remove().
         */
        switch (array.length) {
        case 1: {
            Object table = array[0];
            if (table != null) {
                return Collections.singleton((Table) table);
            }
        }
        //$FALL-THROUGH$
        case 0:
            return Collections.emptySet();
        default: {
            HashSet<Table> set = new HashSet<>();
            for (Object table : array) {
                if (table != null) {
                    set.add((Table) table);
                }
            }
            return set;
        }
        }
    }

    /**
     * Wait if the exclusive mode has been enabled for another session. This
     * method returns as soon as the exclusive mode has been disabled.
     */
    public void waitIfExclusiveModeEnabled() {
        transitionToState(State.RUNNING, true);
        // Even in exclusive mode, we have to let the LOB session proceed, or we
        // will get deadlocks.
        if (getDatabase().getLobSession() == this) {
            return;
        }
        while (isOpen()) {
            SessionLocal exclusive = getDatabase().getExclusiveSession();
            if (exclusive == null || exclusive == this) {
                break;
            }
            if (exclusive.isLockedByCurrentThread()) {
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
     * Get the view cache for this session. There are two caches: the derived
     * table cache (which is only use for a single query, has no bounds, and is
     * cleared after use), and the cache for regular views.
     *
     * @param derivedTable
     *            true to get the cache of derived tables
     * @return the view cache or derived table cache
     */
    public Map<Object, QueryExpressionIndex> getViewIndexCache(boolean derivedTable) {
        if (derivedTable) {
            // for derived tables we don't need to use LRU because the cache
            // should not grow too large for a single query (we drop the whole
            // cache in this cache is dropped at the end of prepareLocal)
            if (derivedTableIndexCache == null) {
                derivedTableIndexCache = new HashMap<>();
            }
            return derivedTableIndexCache;
        }
        SmallLRUCache<Object, QueryExpressionIndex> cache = viewIndexCache;
        if (cache == null) {
            viewIndexCache = cache = SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
        }
        return cache;
    }

    public void setQueryTimeout(int queryTimeout) {
        int max = getDatabase().getSettings().maxQueryTimeout;
        if (max != 0 && (max < queryTimeout || queryTimeout == 0)) {
            // the value must be at most max
            queryTimeout = max;
        }
        this.queryTimeout = queryTimeout;
        // must reset the cancel at here,
        // otherwise it is still used
        cancelAtNs = 0L;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set the table this session is waiting for, and the thread that is
     * waiting.
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

    public Value getTransactionId() {
        if (transaction == null || !transaction.hasChanges()) {
            return ValueNull.INSTANCE;
        }
        return ValueVarchar.get(Long.toString(transaction.getSequenceNum()));
    }

    /**
     * Get the next object id.
     *
     * @return the next object id
     */
    public int nextObjectId() {
        return objectId++;
    }

    /**
     * Get the transaction to use for this session.
     *
     * @return the transaction
     */
    public Transaction getTransaction() {
        if (transaction == null) {
            Store store = getDatabase().getStore();
            if (store.getMvStore().isClosed()) {
                Throwable backgroundException = getDatabase().getBackgroundException();
                getDatabase().shutdownImmediately();
                throw DbException.get(ErrorCode.DATABASE_IS_CLOSED, backgroundException);
            }
            transaction = store.getTransactionStore().begin(this, this.lockTimeout, id, isolationLevel);
            startStatement = -1;
        }
        return transaction;
    }

    private long getStatementSavepoint() {
        if (startStatement == -1) {
            startStatement = getTransaction().setSavepoint();
        }
        return startStatement;
    }

    /**
     * Start a new statement within a transaction.
     * @param command about to be started
     */
    @SuppressWarnings("incomplete-switch")
    public void startStatementWithinTransaction(Command command) {
        Transaction transaction = getTransaction();
        if (transaction != null) {
            HashSet<MVMap<Object,VersionedValue<Object>>> maps = new HashSet<>();
            if (command != null) {
                Set<DbObject> dependencies = command.getDependencies();
                switch (transaction.getIsolationLevel()) {
                case SNAPSHOT:
                case SERIALIZABLE:
                    if (!transaction.hasStatementDependencies()) {
                        for (Schema schema : getDatabase().getAllSchemasNoMeta()) {
                            for (Table table : schema.getAllTablesAndViews(null)) {
                                if (table instanceof MVTable) {
                                    addTableToDependencies((MVTable)table, maps);
                                }
                            }
                        }
                        break;
                    }
                    //$FALL-THROUGH$
                case READ_COMMITTED:
                case READ_UNCOMMITTED:
                    for (DbObject dependency : dependencies) {
                        if (dependency instanceof MVTable) {
                            addTableToDependencies((MVTable)dependency, maps);
                        }
                    }
                    break;
                case REPEATABLE_READ:
                    HashSet<MVTable> processed = new HashSet<>();
                    for (DbObject dependency : dependencies) {
                        if (dependency instanceof MVTable) {
                            addTableToDependencies((MVTable)dependency, maps, processed);
                        }
                    }
                    break;
                }
            }
            transaction.markStatementStart(maps);
        }
        startStatement = -1;
        if (command != null) {
            setCurrentCommand(command);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void addTableToDependencies(MVTable table, HashSet<MVMap<Object,VersionedValue<Object>>> maps) {
        for (Index index : table.getIndexes()) {
            if (index instanceof MVIndex) {
                maps.add(((MVIndex) index).getMVMap());
            }
        }
    }

    private static void addTableToDependencies(MVTable table, HashSet<MVMap<Object,VersionedValue<Object>>> maps,
            HashSet<MVTable> processed) {
        if (!processed.add(table)) {
            return;
        }
        addTableToDependencies(table, maps);
        ArrayList<Constraint> constraints = table.getConstraints();
        if (constraints != null) {
            for (Constraint constraint : constraints) {
                Table ref = constraint.getTable();
                if (ref != table && ref instanceof MVTable) {
                    addTableToDependencies((MVTable) ref, maps, processed);
                }
            }
        }
    }

    /**
     * Mark the statement as completed. This also close all temporary result
     * set, and deletes all temporary files held by the result sets.
     */
    public void endStatement() {
        setCurrentCommand(null);
        if (hasTransaction()) {
            transaction.markStatementEnd();
        }
        startStatement = -1;
    }

    /**
     * Clear the view cache for this session.
     */
    public void clearViewIndexCache() {
        viewIndexCache = null;
    }

    @Override
    public ValueLob addTemporaryLob(ValueLob v) {
        LobData lobData = v.getLobData();
        if (lobData instanceof LobDataInMemory) {
            return v;
        }
        int tableId = ((LobDataDatabase) lobData).getTableId();
        if (tableId == LobStorageFrontend.TABLE_RESULT || tableId == LobStorageFrontend.TABLE_TEMP) {
            if (temporaryResultLobs == null) {
                temporaryResultLobs = new LinkedList<>();
            }
            temporaryResultLobs.add(new TimeoutValue(v));
        } else {
            if (temporaryLobs == null) {
                temporaryLobs = new ArrayList<>();
            }
            temporaryLobs.add(v);
        }
        return v;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    /**
     * Mark that the given table needs to be analyzed on commit.
     *
     * @param table the table
     */
    public void markTableForAnalyze(Table table) {
        if (tablesToAnalyze == null) {
            tablesToAnalyze = new HashSet<>();
        }
        tablesToAnalyze.add(table);
    }

    public State getState() {
        return getBlockingSessionId() != 0 ? State.BLOCKED : state.get();
    }

    public int getBlockingSessionId() {
        return transaction == null ? 0 : transaction.getBlockerId();
    }

    @Override
    public void onRollback(MVMap<Object, VersionedValue<Object>> map, Object key,
                            VersionedValue<Object> existingValue,
                            VersionedValue<Object> restoredValue) {
        // Here we are relying on the fact that map which backs table's primary index
        // has the same name as the table itself
        Store store = getDatabase().getStore();
        MVTable table = store.getTable(map.getName());
        if (table != null) {
            Row oldRow = existingValue == null ? null : (Row) existingValue.getCurrentValue();
            Row newRow = restoredValue == null ? null : (Row) restoredValue.getCurrentValue();
            table.fireAfterRow(this, oldRow, newRow, true);

            if (table.getContainsLargeObject()) {
                if (oldRow != null) {
                    for (int i = 0, len = oldRow.getColumnCount(); i < len; i++) {
                        Value v = oldRow.getValue(i);
                        if (v instanceof ValueLob) {
                            removeAtCommit((ValueLob) v);
                        }
                    }
                }
                if (newRow != null) {
                    for (int i = 0, len = newRow.getColumnCount(); i < len; i++) {
                        Value v = newRow.getValue(i);
                        if (v instanceof ValueLob) {
                            removeAtCommitStop((ValueLob) v);
                        }
                    }
                }
            }
        }
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

    /**
     * An LOB object with a timeout.
     */
    public static class TimeoutValue {

        /**
         * The time when this object was created.
         */
        final long created = System.nanoTime();

        /**
         * The value.
         */
        final ValueLob value;

        TimeoutValue(ValueLob v) {
            this.value = v;
        }

    }

    /**
     * Returns the network connection information, or {@code null}.
     *
     * @return the network connection information, or {@code null}
     */
    public NetworkConnectionInfo getNetworkConnectionInfo() {
        return networkConnectionInfo;
    }

    @Override
    public void setNetworkConnectionInfo(NetworkConnectionInfo networkConnectionInfo) {
        this.networkConnectionInfo = networkConnectionInfo;
    }

    @Override
    public ValueTimestampTimeZone currentTimestamp() {
        ValueTimestampTimeZone ts = currentTimestamp;
        if (ts == null) {
            currentTimestamp = ts = DateTimeUtils.currentTimestamp(timeZone, commandStartOrEnd);
        }
        return ts;
    }

    @Override
    public Mode getMode() {
        return getDatabase().getMode();
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        return getDatabase().getJavaObjectSerializer();
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public void setIsolationLevel(IsolationLevel isolationLevel) {
        commit(false);
        this.isolationLevel = isolationLevel;
    }

    /**
     * Gets bit set of non-keywords.
     *
     * @return set of non-keywords, or {@code null}
     */
    public BitSet getNonKeywords() {
        return nonKeywords;
    }

    /**
     * Sets bit set of non-keywords.
     *
     * @param nonKeywords set of non-keywords, or {@code null}
     */
    public void setNonKeywords(BitSet nonKeywords) {
        this.nonKeywords = nonKeywords;
    }

    @Override
    public StaticSettings getStaticSettings() {
        StaticSettings settings = staticSettings;
        if (settings == null) {
            DbSettings dbSettings = getDatabase().getSettings();
            staticSettings = settings = new StaticSettings(dbSettings.databaseToUpper, dbSettings.databaseToLower,
                    dbSettings.caseInsensitiveIdentifiers);
        }
        return settings;
    }

    @Override
    public DynamicSettings getDynamicSettings() {
        return new DynamicSettings(getMode(), timeZone);
    }

    @Override
    public TimeZoneProvider currentTimeZone() {
        return timeZone;
    }

    /**
     * Sets current time zone.
     *
     * @param timeZone time zone
     */
    public void setTimeZone(TimeZoneProvider timeZone) {
        if (!timeZone.equals(this.timeZone)) {
            this.timeZone = timeZone;
            ValueTimestampTimeZone ts = currentTimestamp;
            if (ts != null) {
                long dateValue = ts.getDateValue();
                long timeNanos = ts.getTimeNanos();
                int offsetSeconds = ts.getTimeZoneOffsetSeconds();
                currentTimestamp = DateTimeUtils.timestampTimeZoneAtOffset(dateValue, timeNanos, offsetSeconds, //
                        timeZone.getTimeZoneOffsetUTC(
                                DateTimeUtils.getEpochSeconds(dateValue, timeNanos, offsetSeconds)));
            }
            modificationId++;
        }
    }

    /**
     * Check if two values are equal with the current comparison mode.
     *
     * @param a the first value
     * @param b the second value
     * @return true if both objects are equal
     */
    public boolean areEqual(Value a, Value b) {
        // can not use equals because ValueDecimal 0.0 is not equal to 0.00.
        return a.compareTo(b, this, getCompareMode()) == 0;
    }

    /**
     * Compare two values with the current comparison mode. The values may have
     * different data types including NULL.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compare(Value a, Value b) {
        return a.compareTo(b, this, getCompareMode());
    }

    /**
     * Compare two values with the current comparison mode. The values may have
     * different data types including NULL.
     *
     * @param a the first value
     * @param b the second value
     * @param forEquality perform only check for equality (= or &lt;&gt;)
     * @return 0 if both values are equal, -1 if the first value is smaller, 1
     *         if the second value is larger, {@link Integer#MIN_VALUE} if order
     *         is not defined due to NULL comparison
     */
    public int compareWithNull(Value a, Value b, boolean forEquality) {
        return a.compareWithNull(b, forEquality, this, getCompareMode());
    }

    /**
     * Compare two values with the current comparison mode. The values must be
     * of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSafe(Value a, Value b) {
        return a.compareTypeSafe(b, getCompareMode(), this);
    }

    /**
     * Changes parsing mode of data types with too large length.
     *
     * @param truncateLargeLength
     *            {@code true} to truncate to valid bound, {@code false} to
     *            throw an exception
     */
    public void setTruncateLargeLength(boolean truncateLargeLength) {
        this.truncateLargeLength = truncateLargeLength;
    }

    /**
     * Returns parsing mode of data types with too large length.
     *
     * @return {@code true} if large length is truncated, {@code false} if an
     *         exception is thrown
     */
    public boolean isTruncateLargeLength() {
        return truncateLargeLength;
    }

    /**
     * Changes parsing of a BINARY data type.
     *
     * @param variableBinary
     *            {@code true} to parse BINARY as VARBINARY, {@code false} to
     *            parse it as is
     */
    public void setVariableBinary(boolean variableBinary) {
        this.variableBinary = variableBinary;
    }

    /**
     * Returns BINARY data type parsing mode.
     *
     * @return {@code true} if BINARY should be parsed as VARBINARY,
     *         {@code false} if it should be parsed as is
     */
    public boolean isVariableBinary() {
        return variableBinary;
    }

    /**
     * Changes INFORMATION_SCHEMA content.
     *
     * @param oldInformationSchema
     *            {@code true} to have old-style tables in INFORMATION_SCHEMA,
     *            {@code false} to have modern tables
     */
    public void setOldInformationSchema(boolean oldInformationSchema) {
        this.oldInformationSchema = oldInformationSchema;
    }

    @Override
    public boolean isOldInformationSchema() {
        return oldInformationSchema;
    }

    @Override
    public DatabaseMeta getDatabaseMeta() {
        return new DatabaseMetaLocal(this);
    }

    @Override
    public boolean zeroBasedEnums() {
        return getDatabase().zeroBasedEnums();
    }

    /**
     * Enables or disables the quirks mode.
     *
     * @param quirksMode
     *            whether quirks mode should be enabled
     */
    public void setQuirksMode(boolean quirksMode) {
        this.quirksMode = quirksMode;
    }

    /**
     * Returns whether quirks mode is enabled explicitly or implicitly.
     *
     * @return {@code true} if database is starting or quirks mode was enabled
     *         explicitly, {@code false} otherwise
     */
    public boolean isQuirksMode() {
        return quirksMode || getDatabase().isStarting();
    }

    @Override
    public Session setThreadLocalSession() {
        Session oldSession = THREAD_LOCAL_SESSION.get();
        THREAD_LOCAL_SESSION.set(this);
        return oldSession;
    }

    @Override
    public void resetThreadLocalSession(Session oldSession) {
        if (oldSession == null) {
            THREAD_LOCAL_SESSION.remove();
        } else {
            THREAD_LOCAL_SESSION.set(oldSession);
        }
    }

    private CompareMode getCompareMode() {
        return getDatabase().getCompareMode();
    }

    private <T> HashMap<String,T> newStringsMap() {
        return getDatabase().newStringMap();
    }
}
