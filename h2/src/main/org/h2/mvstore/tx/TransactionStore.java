/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.h2.engine.IsolationLevel;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RootReference;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.SpatialDataType;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.mvstore.type.MetaType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.mvstore.type.StringDataType;
import org.h2.util.StringUtils;
import org.h2.value.VersionedValue;

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public class TransactionStore {

    /**
     * The store.
     */
    final MVStore store;

    /**
     * Default blocked transaction timeout
     */
    final int timeoutMillis;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    private final MVMap<Integer, Object[]> preparedTransactions;

    private final MVMap<String, DataType<?>> typeRegistry;

    /**
     * Undo logs.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    @SuppressWarnings("unchecked")
    final MVMap<Long,Record<?,?>>[] undoLogs = new MVMap[MAX_OPEN_TRANSACTIONS];
    private final MVMap.Builder<Long, Record<?,?>> undoLogBuilder;

    private final DataType<?> dataType;

    /**
     * This BitSet is used as vacancy indicator for transaction slots in transactions[].
     * It provides easy way to find first unoccupied slot, and also allows for copy-on-write
     * non-blocking updates.
     */
    final AtomicReference<VersionedBitSet> openTransactions = new AtomicReference<>(new VersionedBitSet());

    /**
     * This is intended to be the source of ultimate truth about transaction being committed.
     * Once bit is set, corresponding transaction is logically committed,
     * although it might be plenty of "uncommitted" entries in various maps
     * and undo record are still around.
     * Nevertheless, all of those should be considered by other transactions as committed.
     */
    final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());

    private boolean init;

    /**
     * Soft limit on the number of concurrently opened transactions.
     * Not really needed but used by some test.
     */
    private int maxTransactionId = MAX_OPEN_TRANSACTIONS;

    /**
     * Array holding all open transaction objects.
     * Position in array is "transaction id".
     * VolatileReferenceArray would do the job here, but there is no such thing in Java yet
     */
    private final AtomicReferenceArray<Transaction> transactions =
                                                        new AtomicReferenceArray<>(MAX_OPEN_TRANSACTIONS + 1);

    private static final String TYPE_REGISTRY_NAME = "_";

    /**
     * The prefix for undo log entries.
     */
    public static final String UNDO_LOG_NAME_PREFIX = "undoLog";

    // must come before open in lexicographical order
    private static final char UNDO_LOG_COMMITTED = '-';

    private static final char UNDO_LOG_OPEN = '.';

    /**
     * Hard limit on the number of concurrently opened transactions
     */
    // TODO: introduce constructor parameter instead of a static field, driven by URL parameter
    private static final int MAX_OPEN_TRANSACTIONS = 65535;

    /**
     * Generate a string used to name undo log map for a specific transaction.
     * This name will contain transaction id.
     *
     * @param transactionId of the corresponding transaction
     * @return undo log name
     */
    private static String getUndoLogName(int transactionId) {
        return transactionId > 0 ? UNDO_LOG_NAME_PREFIX + UNDO_LOG_OPEN + transactionId
                : UNDO_LOG_NAME_PREFIX + UNDO_LOG_OPEN;
    }

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new ObjectDataType());
    }

    public TransactionStore(MVStore store, DataType<?> dataType) {
        this(store, new MetaType<>(null, store.backgroundExceptionHandler), dataType, 0);
    }

    /**
     * Create a new transaction store.
     * @param store the store
     * @param metaDataType the data type for type registry map values
     * @param dataType default data type for map keys and values
     * @param timeoutMillis lock acquisition timeout in milliseconds, 0 means no wait
     */
    public TransactionStore(MVStore store, MetaType<?> metaDataType, DataType<?> dataType, int timeoutMillis) {
        this.store = store;
        this.dataType = dataType;
        this.timeoutMillis = timeoutMillis;
        this.typeRegistry = openTypeRegistry(store, metaDataType);
        this.preparedTransactions = store.openMap("openTransactions", new MVMap.Builder<>());
        this.undoLogBuilder = createUndoLogBuilder();
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    MVMap.Builder<Long,Record<?, ?>> createUndoLogBuilder() {
        return new MVMap.Builder<Long,Record<?,?>>()
                .singleWriter()
                .keyType(LongDataType.INSTANCE)
                .valueType(new Record.Type(this));
    }

    private static MVMap<String, DataType<?>> openTypeRegistry(MVStore store, MetaType<?> metaDataType) {
        MVMap.Builder<String, DataType<?>> typeRegistryBuilder =
                                    new MVMap.Builder<String, DataType<?>>()
                                                .keyType(StringDataType.INSTANCE)
                                                .valueType(metaDataType);
        return store.openMap(TYPE_REGISTRY_NAME, typeRegistryBuilder);
    }

    /**
     * Initialize the store without any RollbackListener.
     * @see #init(RollbackListener)
     */
    public void init() {
        init(ROLLBACK_LISTENER_NONE);
    }

    /**
     * Initialize the store. This is needed before a transaction can be opened.
     * If the transaction store is corrupt, this method can throw an exception,
     * in which case the store can only be used for reading.
     *
     * @param listener to notify about transaction rollback
     */
    public void init(RollbackListener listener) {
        if (!init) {
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith(UNDO_LOG_NAME_PREFIX)) {
                    // Unexpectedly short name may be encountered upon upgrade from older version
                    // where undo log was persisted as a single map, remove it.
                    if (mapName.length() > UNDO_LOG_NAME_PREFIX.length()) {
                        // make a decision about tx status based on a log name
                        // to handle upgrade from a previous versions
                        boolean committed = mapName.charAt(UNDO_LOG_NAME_PREFIX.length()) == UNDO_LOG_COMMITTED;
                        if (store.hasData(mapName)) {
                            int transactionId = StringUtils.parseUInt31(mapName, UNDO_LOG_NAME_PREFIX.length() + 1,
                                    mapName.length());
                            VersionedBitSet openTxBitSet = openTransactions.get();
                            if (!openTxBitSet.get(transactionId)) {
                                Object[] data = preparedTransactions.get(transactionId);
                                int status;
                                String name;
                                if (data == null) {
                                    status = Transaction.STATUS_OPEN;
                                    name = null;
                                } else {
                                    status = (Integer) data[0];
                                    name = (String) data[1];
                                }
                                MVMap<Long, Record<?,?>> undoLog = store.openMap(mapName, undoLogBuilder);
                                undoLogs[transactionId] = undoLog;
                                Long lastUndoKey = undoLog.lastKey();
                                assert lastUndoKey != null;
                                assert getTransactionId(lastUndoKey) == transactionId;
                                long logId = getLogId(lastUndoKey) + 1;
                                if (committed) {
                                    // give it a proper name and used marker record instead
                                    store.renameMap(undoLog, getUndoLogName(transactionId));
                                    markUndoLogAsCommitted(transactionId);
                                } else {
                                    committed = logId > LOG_ID_MASK;
                                }
                                if (committed) {
                                    status = Transaction.STATUS_COMMITTED;
                                    lastUndoKey = undoLog.lowerKey(lastUndoKey);
                                    assert lastUndoKey == null || getTransactionId(lastUndoKey) == transactionId;
                                    logId = lastUndoKey == null ? 0 : getLogId(lastUndoKey) + 1;
                                }
                                registerTransaction(transactionId, status, name, logId, timeoutMillis, 0,
                                        IsolationLevel.READ_COMMITTED, listener);
                                continue;
                            }
                        }
                    }

                    if (!store.isReadOnly()) {
                        store.removeMap(mapName);
                    }
                }
            }
            init = true;
        }
    }

    private void markUndoLogAsCommitted(int transactionId) {
        addUndoLogRecord(transactionId, LOG_ID_MASK, Record.COMMIT_MARKER);
    }

    /**
     * Commit all transactions that are in the committed state, and
     * rollback all open transactions.
     */
    public void endLeftoverTransactions() {
        List<Transaction> list = getOpenTransactions();
        for (Transaction t : list) {
            int status = t.getStatus();
            if (status == Transaction.STATUS_COMMITTED) {
                t.commit();
            } else if (status != Transaction.STATUS_PREPARED) {
                t.rollback();
            }
        }
    }

    int getMaxTransactionId() {
        return maxTransactionId;
    }

    /**
     * Set the maximum transaction id, after which ids are re-used. If the old
     * transaction is still in use when re-using an old id, the new transaction
     * fails.
     *
     * @param max the maximum id
     */
    public void setMaxTransactionId(int max) {
        DataUtils.checkArgument(max <= MAX_OPEN_TRANSACTIONS,
                "Concurrent transactions limit is too high: {0}", max);
        this.maxTransactionId = max;
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return store.hasMap(name);
    }

    private static final int LOG_ID_BITS = Transaction.LOG_ID_BITS;
    private static final long LOG_ID_MASK = (1L << LOG_ID_BITS) - 1;

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << (64 - LOG_ID_BITS)),
                "Transaction id out of range: {0}", transactionId);
        DataUtils.checkArgument(logId >= 0 && logId <= LOG_ID_MASK,
                "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << LOG_ID_BITS) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    static int getTransactionId(long operationId) {
        return (int) (operationId >>> LOG_ID_BITS);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & LOG_ID_MASK;
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    public List<Transaction> getOpenTransactions() {
        if(!init) {
            init();
        }
        ArrayList<Transaction> list = new ArrayList<>();
        int transactionId = 0;
        BitSet bitSet = openTransactions.get();
        while((transactionId = bitSet.nextSetBit(transactionId + 1)) > 0) {
            Transaction transaction = getTransaction(transactionId);
            if(transaction != null) {
                if(transaction.getStatus() != Transaction.STATUS_CLOSED) {
                    list.add(transaction);
                }
            }
        }
        return list;
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        store.commit();
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public Transaction begin() {
        return begin(ROLLBACK_LISTENER_NONE, timeoutMillis, 0, IsolationLevel.READ_COMMITTED);
    }

    /**
     * Begin a new transaction.
     * @param listener to be notified in case of a rollback
     * @param timeoutMillis to wait for a blocking transaction
     * @param ownerId of the owner (Session?) to be reported by getBlockerId
     * @param isolationLevel of new transaction
     * @return the transaction
     */
    public Transaction begin(RollbackListener listener, int timeoutMillis, int ownerId,
            IsolationLevel isolationLevel) {
        Transaction transaction = registerTransaction(0, Transaction.STATUS_OPEN, null, 0,
                timeoutMillis, ownerId, isolationLevel, listener);
        return transaction;
    }

    private Transaction registerTransaction(int txId, int status, String name, long logId,
                                            int timeoutMillis, int ownerId,
                                            IsolationLevel isolationLevel, RollbackListener listener) {
        int transactionId;
        long sequenceNo;
        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            if (txId == 0) {
                transactionId = original.nextClearBit(1);
            } else {
                transactionId = txId;
                assert !original.get(transactionId);
            }
            if (transactionId > maxTransactionId) {
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                        "There are {0} open transactions",
                        transactionId - 1);
            }
            VersionedBitSet clone = original.clone();
            clone.set(transactionId);
            sequenceNo = clone.getVersion() + 1;
            clone.setVersion(sequenceNo);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        Transaction transaction = new Transaction(this, transactionId, sequenceNo, status, name, logId,
                timeoutMillis, ownerId, isolationLevel, listener);

        assert transactions.get(transactionId) == null;
        transactions.set(transactionId, transaction);

        if (undoLogs[transactionId] == null) {
            String undoName = getUndoLogName(transactionId);
            MVMap<Long,Record<?,?>> undoLog = store.openMap(undoName, undoLogBuilder);
            undoLogs[transactionId] = undoLog;
        }
        return transaction;
    }

    /**
     * Store a transaction.
     *
     * @param t the transaction
     */
    void storeTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED ||
                t.getName() != null) {
            Object[] v = { t.getStatus(), t.getName() };
            preparedTransactions.put(t.getId(), v);
            t.wasStored = true;
        }
    }

    /**
     * Add an undo log entry.
     *
     * @param transactionId id of the transaction
     * @param logId sequential number of the log record within transaction
     * @param record Record(mapId, key, previousValue) to add
     * @return key for the added record
     */
    long addUndoLogRecord(int transactionId, long logId, Record<?,?> record) {
        MVMap<Long, Record<?,?>> undoLog = undoLogs[transactionId];
        long undoKey = getOperationId(transactionId, logId);
        if (logId == 0 && !undoLog.isEmpty()) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                    "An old transaction with the same id " +
                    "is still open: {0}",
                    transactionId);
        }
        undoLog.append(undoKey, record);
        return undoKey;
    }

    /**
     * Remove an undo log entry.
     * @param transactionId id of the transaction
     */
    void removeUndoLogRecord(int transactionId) {
        undoLogs[transactionId].trimLast();
    }

    /**
     * Remove the given map.
     *
     * @param map the map
     */
    void removeMap(TransactionMap<?,?> map) {
        store.removeMap(map.map);
    }

    /**
     * Commit a transaction.
     *  @param t transaction to commit
     *  @param recovery if called during initial transaction recovery procedure
     *                  therefore undo log is stored under "committed" name already
     */
    void commit(Transaction t, boolean recovery) {
        if (!store.isClosed()) {
            int transactionId = t.transactionId;
            // First, mark log as "committed".
            // It does not change the way this transaction is treated by others,
            // but preserves fact of commit in case of abrupt termination.
            MVMap<Long,Record<?,?>> undoLog = undoLogs[transactionId];
            Cursor<Long,Record<?,?>> cursor;
            if(recovery) {
                removeUndoLogRecord(transactionId);
                cursor = undoLog.cursor(null);
            } else {
                cursor = undoLog.cursor(null);
                markUndoLogAsCommitted(transactionId);
            }

            // this is an atomic action that causes all changes
            // made by this transaction, to be considered as "committed"
            flipCommittingTransactionsBit(transactionId, true);

            CommitDecisionMaker<Object> commitDecisionMaker = new CommitDecisionMaker<>();
            try {
                while (cursor.hasNext()) {
                    Long undoKey = cursor.next();
                    Record<?,?> op = cursor.getValue();
                    int mapId = op.mapId;
                    MVMap<Object, VersionedValue<Object>> map = openMap(mapId);
                    if (map != null && !map.isClosed()) { // might be null if map was removed later
                        Object key = op.key;
                        commitDecisionMaker.setUndoKey(undoKey);
                        // second parameter (value) is not really
                        // used by CommitDecisionMaker
                        map.operate(key, null, commitDecisionMaker);
                    }
                }
            } finally {
                try {
                    undoLog.clear();
                } finally {
                    flipCommittingTransactionsBit(transactionId, false);
                }
            }
        }
    }

    private void flipCommittingTransactionsBit(int transactionId, boolean flag) {
        boolean success;
        do {
            BitSet original = committingTransactions.get();
            assert original.get(transactionId) != flag : flag ? "Double commit" : "Mysterious bit's disappearance";
            BitSet clone = (BitSet) original.clone();
            clone.set(transactionId, flag);
            success = committingTransactions.compareAndSet(original, clone);
        } while(!success);
    }

    <K,V> MVMap<K, VersionedValue<V>> openVersionedMap(String name, DataType<K> keyType, DataType<V> valueType) {
        VersionedValueType<V,?> vt = valueType == null ? null : new VersionedValueType<>(valueType);
        return openMap(name, keyType, vt);
    }

    /**
     * Open the map with the given name.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the map name
     * @param keyType the key type
     * @param valueType the value type
     * @return the map
     */
    public <K,V> MVMap<K, V> openMap(String name, DataType<K> keyType, DataType<V> valueType) {
        return store.openMap(name, new TxMapBuilder<K, V>(typeRegistry, dataType)
                                            .keyType(keyType).valueType(valueType));
    }

    /**
     * Open the map with the given id.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param mapId the id
     * @return the map
     */
    <K,V> MVMap<K, VersionedValue<V>> openMap(int mapId) {
        MVMap<K, VersionedValue<V>> map = store.getMap(mapId);
        if (map == null) {
            String mapName = store.getMapName(mapId);
            if (mapName == null) {
                // the map was removed later on
                return null;
            }
            MVMap.Builder<K, VersionedValue<V>> txMapBuilder = new TxMapBuilder<>(typeRegistry, dataType);
            map = store.openMap(mapId, txMapBuilder);
        }
        return map;
    }

    <K,V> MVMap<K,VersionedValue<V>> getMap(int mapId) {
        MVMap<K, VersionedValue<V>> map = store.getMap(mapId);
        if (map == null && !init) {
            map = openMap(mapId);
        }
        assert map != null : "map with id " + mapId + " is missing" +
                                (init ? "" : " during initialization");
        return map;
    }

    /**
     * End this transaction. Change status to CLOSED and vacate transaction slot.
     * Will try to commit MVStore if autocommitDelay is 0 or if database is idle
     * and amount of unsaved changes is sizable.
     *
     * @param t the transaction
     * @param hasChanges true if transaction has done any updates
     *                  (even if they are fully rolled back),
     *                   false if it just performed a data access
     */
    void endTransaction(Transaction t, boolean hasChanges) {
        t.closeIt();
        int txId = t.transactionId;
        transactions.set(txId, null);

        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            assert original.get(txId);
            VersionedBitSet clone = original.clone();
            clone.clear(txId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        if (hasChanges) {
            boolean wasStored = t.wasStored;
            if (wasStored && !preparedTransactions.isClosed()) {
                preparedTransactions.remove(txId);
            }

            if (store.isVersioningRequired()) {
                if (wasStored || store.getAutoCommitDelay() == 0) {
                    store.commit();
                } else {
                    if (isUndoEmpty()) {
                        // to avoid having to store the transaction log,
                        // if there is no open transaction,
                        // and if there have been many changes, store them now
                        int unsaved = store.getUnsavedMemory();
                        int max = store.getAutoCommitMemory();
                        // save at 3/4 capacity
                        if (unsaved * 4 > max * 3) {
                            store.tryCommit();
                        }
                    }
                }
            }
        }
    }

    /**
     * Get the root references (snapshots) for undo-log maps.
     * Those snapshots can potentially be used to optimize TransactionMap.size().
     *
     * @return the array of root references or null if snapshotting is not possible
     */
    RootReference<Long,Record<?,?>>[] collectUndoLogRootReferences() {
        BitSet opentransactions = openTransactions.get();
        @SuppressWarnings("unchecked")
        RootReference<Long,Record<?,?>>[] undoLogRootReferences = new RootReference[opentransactions.length()];
        for (int i = opentransactions.nextSetBit(0); i >= 0; i = opentransactions.nextSetBit(i+1)) {
            MVMap<Long,Record<?,?>> undoLog = undoLogs[i];
            if (undoLog != null) {
                RootReference<Long,Record<?,?>> rootReference = undoLog.getRoot();
                if (rootReference.needFlush()) {
                    // abort attempt to collect snapshots for all undo logs
                    // because map's append buffer can't be flushed from a non-owning thread
                    return null;
                }
                undoLogRootReferences[i] = rootReference;
            }
        }
        return undoLogRootReferences;
    }

    /**
     * Calculate the size for undo log entries.
     *
     * @param undoLogRootReferences the root references
     * @return the number of key-value pairs
     */
    static long calculateUndoLogsTotalSize(RootReference<Long,Record<?,?>>[] undoLogRootReferences) {
        long undoLogsTotalSize = 0;
        for (RootReference<Long,Record<?,?>> rootReference : undoLogRootReferences) {
            if (rootReference != null) {
                undoLogsTotalSize += rootReference.getTotalCount();
            }
        }
        return undoLogsTotalSize;
    }

    private boolean isUndoEmpty() {
        BitSet openTrans = openTransactions.get();
        for (int i = openTrans.nextSetBit(0); i >= 0; i = openTrans.nextSetBit(i + 1)) {
            MVMap<Long,Record<?,?>> undoLog = undoLogs[i];
            if (undoLog != null && !undoLog.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get Transaction object for a transaction id.
     *
     * @param transactionId id for an open transaction
     * @return Transaction object.
     */
    Transaction getTransaction(int transactionId) {
        return transactions.get(transactionId);
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        int transactionId = t.getId();
        MVMap<Long,Record<?,?>> undoLog = undoLogs[transactionId];
        RollbackDecisionMaker decisionMaker = new RollbackDecisionMaker(this, transactionId, toLogId, t.listener);
        for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
            Long undoKey = getOperationId(transactionId, logId);
            undoLog.operate(undoKey, null, decisionMaker);
            decisionMaker.reset();
        }
    }

    /**
     * Get the changes of the given transaction, starting from the latest log id
     * back to the given log id.
     *
     * @param t the transaction
     * @param maxLogId the maximum log id
     * @param toLogId the minimum log id
     * @return the changes
     */
    Iterator<Change> getChanges(final Transaction t, final long maxLogId,
            final long toLogId) {

        final MVMap<Long,Record<?,?>> undoLog = undoLogs[t.getId()];
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            private void fetchNext() {
                int transactionId = t.getId();
                while (logId >= toLogId) {
                    Long undoKey = getOperationId(transactionId, logId);
                    Record<?,?> op = undoLog.get(undoKey);
                    logId--;
                    if (op == null) {
                        // partially rolled back: load previous
                        undoKey = undoLog.floorKey(undoKey);
                        if (undoKey == null || getTransactionId(undoKey) != transactionId) {
                            break;
                        }
                        logId = getLogId(undoKey);
                        continue;
                    }
                    int mapId = op.mapId;
                    MVMap<Object, VersionedValue<Object>> m = openMap(mapId);
                    if (m != null) { // could be null if map was removed later on
                        VersionedValue<?> oldValue = op.oldValue;
                        current = new Change(m.getName(), op.key,
                                oldValue == null ? null : oldValue.getCurrentValue());
                        return;
                    }
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                if(current == null) {
                    fetchNext();
                }
                return current != null;
            }

            @Override
            public Change next() {
                if(!hasNext()) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                current = null;
                return result;
            }

        };
    }

    /**
     * A change in a map.
     */
    public static class Change {

        /**
         * The name of the map where the change occurred.
         */
        public final String mapName;

        /**
         * The key.
         */
        public final Object key;

        /**
         * The value.
         */
        public final Object value;

        public Change(String mapName, Object key, Object value) {
            this.mapName = mapName;
            this.key = key;
            this.value = value;
        }
    }

    /**
     * This listener can be registered with the transaction to be notified of
     * every compensating change during transaction rollback.
     * Normally this is not required, if no external resources were modified,
     * because state of all transactional maps will be restored automatically.
     * Only state of external resources, possibly modified by triggers
     * need to be restored.
     */
    public interface RollbackListener {

        /**
         * Notified of a single map change (add/update/remove)
         * @param map modified
         * @param key of the modified entry
         * @param existingValue value in the map (null if delete is rolled back)
         * @param restoredValue value to be restored (null if add is rolled back)
         */
        void onRollback(MVMap<Object,VersionedValue<Object>> map, Object key,
                        VersionedValue<Object> existingValue, VersionedValue<Object> restoredValue);
    }

    private static final RollbackListener ROLLBACK_LISTENER_NONE = (map, key, existingValue, restoredValue) -> {};

    private static final class TxMapBuilder<K,V> extends MVMap.Builder<K,V> {

        private final MVMap<String, DataType<?>> typeRegistry;
        private final DataType defaultDataType;

        TxMapBuilder(MVMap<String, DataType<?>> typeRegistry, DataType<?> defaultDataType) {
            this.typeRegistry = typeRegistry;
            this.defaultDataType = defaultDataType;
        }

        private void registerDataType(DataType<?> dataType) {
            String key = getDataTypeRegistrationKey(dataType);
            DataType<?> registeredDataType = typeRegistry.putIfAbsent(key, dataType);
            if(registeredDataType != null) {
                // TODO: ensure type consistency
            }
        }

        static String getDataTypeRegistrationKey(DataType<?> dataType) {
            return Integer.toHexString(Objects.hashCode(dataType));
        }

        @SuppressWarnings("unchecked")
        @Override
        public MVMap<K,V> create(MVStore store, Map<String, Object> config) {
            DataType<K> keyType = getKeyType();
            if (keyType == null) {
                String keyTypeKey = (String) config.remove("key");
                if (keyTypeKey != null) {
                    keyType = (DataType<K>)typeRegistry.get(keyTypeKey);
                    if (keyType == null) {
                        throw DataUtils.newMVStoreException(DataUtils.ERROR_UNKNOWN_DATA_TYPE,
                                "Data type with hash {0} can not be found", keyTypeKey);
                    }
                    setKeyType(keyType);
                }
            } else {
                registerDataType(keyType);
            }

            DataType<V> valueType = getValueType();
            if (valueType == null) {
                String valueTypeKey = (String) config.remove("val");
                if (valueTypeKey != null) {
                    valueType = (DataType<V>)typeRegistry.get(valueTypeKey);
                    if (valueType == null) {
                        throw DataUtils.newMVStoreException(DataUtils.ERROR_UNKNOWN_DATA_TYPE,
                                "Data type with hash {0} can not be found", valueTypeKey);
                    }
                    setValueType(valueType);
                }
            } else {
                registerDataType(valueType);
            }

            if (getKeyType() == null) {
                setKeyType(defaultDataType);
                registerDataType(getKeyType());
            }
            if (getValueType() == null) {
                setValueType((DataType<? super V>) new VersionedValueType<V,Object>(defaultDataType));
                registerDataType(getValueType());
            }

            config.put("store", store);
            config.put("key", getKeyType());
            config.put("val", getValueType());
            return create(config);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected MVMap<K,V> create(Map<String,Object> config) {
            if ("rtree".equals(config.get("type"))) {
                MVMap<K, V> map = (MVMap<K, V>) new MVRTreeMap<>(config, (SpatialDataType) getKeyType(),
                        getValueType());
                return map;
            }
            return new TMVMap<>(config, getKeyType(), getValueType());
        }

        private static final class TMVMap<K,V> extends MVMap<K,V> {
            private final String type;

            TMVMap(Map<String, Object> config, DataType<K> keyType, DataType<V> valueType) {
                super(config, keyType, valueType);
                type = (String)config.get("type");
            }

            private TMVMap(MVMap<K, V> source) {
                super(source);
                type = source.getType();
            }

            @Override
            protected MVMap<K, V> cloneIt() {
                return new TMVMap<>(this);
            }

            @Override
            public String getType() {
                return type;
            }

            @Override
            protected String asString(String name) {
                StringBuilder buff = new StringBuilder();
                buff.append(super.asString(name));
                DataUtils.appendMap(buff, "key", getDataTypeRegistrationKey(getKeyType()));
                DataUtils.appendMap(buff, "val", getDataTypeRegistrationKey(getValueType()));
                return buff.toString();
            }
        }
    }
}
