/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.h2.engine.IsolationLevel;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.RootReference;
import org.h2.mvstore.type.DataType;
import org.h2.value.VersionedValue;

/**
 * A map that supports transactions.
 *
 * <p>
 * <b>Methods of this class may be changed at any time without notice.</b> If
 * you use this class directly make sure that your application or library
 * requires exactly the same version of MVStore or H2 jar as the version that
 * you use during its development and build.
 * </p>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class TransactionMap<K, V> extends AbstractMap<K,V> {

    /**
     * The map used for writing (the latest version).
     * <p>
     * Key: key the key of the data.
     * Value: { transactionId, oldVersion, value }
     */
    public final MVMap<K, VersionedValue<V>> map;

    /**
     * The transaction which is used for this map.
     */
    private final Transaction transaction;

    /**
     * Snapshot of this map as of beginning of transaction or
     * first usage within transaction or
     * beginning of the statement, depending on isolation level
     */
    private Snapshot<K,VersionedValue<V>> snapshot;

    /**
     * Snapshot of this map as of beginning of beginning of the statement
     */
    private Snapshot<K,VersionedValue<V>> statementSnapshot;

    /**
     * Indicates whether underlying map was modified from within related transaction
     */
    private boolean hasChanges;

    private final TxDecisionMaker<K,V> txDecisionMaker;
    private final TxDecisionMaker<K,V> ifAbsentDecisionMaker;
    private final TxDecisionMaker<K,V> lockDecisionMaker;


    TransactionMap(Transaction transaction, MVMap<K, VersionedValue<V>> map) {
        this.transaction = transaction;
        this.map = map;
        this.txDecisionMaker = new TxDecisionMaker<>(map.getId(), transaction);
        this.ifAbsentDecisionMaker = new TxDecisionMaker.PutIfAbsentDecisionMaker<>(map.getId(),
                transaction, this::getFromSnapshot);
        this.lockDecisionMaker = transaction.allowNonRepeatableRead()
                ? new TxDecisionMaker.LockDecisionMaker<>(map.getId(), transaction)
                : new TxDecisionMaker.RepeatableReadLockDecisionMaker<>(map.getId(), transaction,
                        map.getValueType(), this::getFromSnapshot);

    }

    /**
     * Get a clone of this map for the given transaction.
     *
     * @param transaction the transaction
     * @return the map
     */
    public TransactionMap<K,V> getInstance(Transaction transaction) {
        return transaction.openMapX(map);
    }

    /**
     * Get the number of entries, as a integer. {@link Integer#MAX_VALUE} is
     * returned if there are more than this entries.
     *
     * @return the number of entries, as an integer
     * @see #sizeAsLong()
     */
    @Override
    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Get the size of the raw map. This includes uncommitted entries, and
     * transiently removed entries, so it is the maximum number of entries.
     *
     * @return the maximum size
     */
    public long sizeAsLongMax() {
        return map.sizeAsLong();
    }

    /**
     * Get the size of the map as seen by this transaction.
     *
     * @return the size
     */
    public long sizeAsLong() {
        IsolationLevel isolationLevel = transaction.getIsolationLevel();
        if (!isolationLevel.allowNonRepeatableRead() && hasChanges) {
            return sizeAsLongRepeatableReadWithChanges();
        }
        // getting coherent picture of the map, committing transactions, and undo logs
        // either from values stored in transaction (never loops in that case),
        // or current values from the transaction store (loops until moment of silence)
        Snapshot<K,VersionedValue<V>> snapshot;
        RootReference<Long,Record<?,?>>[] undoLogRootReferences;
        do {
            snapshot = getSnapshot();
            undoLogRootReferences = getTransaction().getUndoLogRootReferences();
        } while (!snapshot.equals(getSnapshot()));

        RootReference<K,VersionedValue<V>> mapRootReference = snapshot.root;
        long size = mapRootReference.getTotalCount();
        long undoLogsTotalSize = undoLogRootReferences == null ? size
                : TransactionStore.calculateUndoLogsTotalSize(undoLogRootReferences);
        // if we are looking at the map without any uncommitted values
        if (undoLogsTotalSize == 0) {
            return size;
        }
        return adjustSize(undoLogRootReferences, mapRootReference,
                isolationLevel == IsolationLevel.READ_UNCOMMITTED ? null : snapshot.committingTransactions,
                size, undoLogsTotalSize);
    }

    private long adjustSize(RootReference<Long, Record<?, ?>>[] undoLogRootReferences,
            RootReference<K, VersionedValue<V>> mapRootReference, BitSet committingTransactions, long size,
            long undoLogsTotalSize) {
        // Entries describing removals from the map by this transaction and all transactions,
        // which are committed but not closed yet,
        // and entries about additions to the map by other uncommitted transactions were counted,
        // but they should not contribute into total count.
        if (2 * undoLogsTotalSize > size) {
            // the undo log is larger than half of the map - scan the entries of the map directly
            Cursor<K, VersionedValue<V>> cursor = map.cursor(mapRootReference, null, null, false);
            while (cursor.hasNext()) {
                cursor.next();
                VersionedValue<?> currentValue = cursor.getValue();
                assert currentValue != null;
                long operationId = currentValue.getOperationId();
                if (operationId != 0 &&         // skip committed entries
                        isIrrelevant(operationId, currentValue, committingTransactions)) {
                    --size;
                }
            }
        } else {
            assert undoLogRootReferences != null;
            // The undo logs are much smaller than the map - scan all undo logs,
            // and then lookup relevant map entry.
            for (RootReference<Long,Record<?,?>> undoLogRootReference : undoLogRootReferences) {
                if (undoLogRootReference != null) {
                    Cursor<Long, Record<?, ?>> cursor = undoLogRootReference.root.map.cursor(undoLogRootReference,
                            null, null, false);
                    while (cursor.hasNext()) {
                        cursor.next();
                        Record<?,?> op = cursor.getValue();
                        if (op.mapId == map.getId()) {
                            @SuppressWarnings("unchecked")
                            VersionedValue<V> currentValue = map.get(mapRootReference.root, (K)op.key);
                            // If map entry is not there, then we never counted
                            // it, in the first place, so skip it.
                            // This is possible when undo entry exists because
                            // it belongs to a committed but not yet closed
                            // transaction, and it was later deleted by some
                            // other already committed and closed transaction.
                            if (currentValue != null) {
                                // only the last undo entry for any given map
                                // key should be considered
                                long operationId = cursor.getKey();
                                assert operationId != 0;
                                if (currentValue.getOperationId() == operationId &&
                                        isIrrelevant(operationId, currentValue, committingTransactions)) {
                                    --size;
                                }
                            }
                        }
                    }
                }
            }
        }
        return size;
    }

    private boolean isIrrelevant(long operationId, VersionedValue<?> currentValue, BitSet committingTransactions) {
        Object v;
        if (committingTransactions == null) {
            v = currentValue.getCurrentValue();
        } else {
            int txId = TransactionStore.getTransactionId(operationId);
            v = txId == transaction.transactionId || committingTransactions.get(txId)
                    ? currentValue.getCurrentValue() : currentValue.getCommittedValue();
        }
        return v == null;
    }

    private long sizeAsLongRepeatableReadWithChanges() {
        long count = 0L;
        RepeatableIterator<K, V, K> iterator = new RepeatableIterator<>(this, null, null, false, false);
        while (iterator.fetchNext() != null) {
            count++;
        }
        return count;
    }

    /**
     * Remove an entry.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @throws MVStoreException if a lock timeout occurs
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        return set((K)key, (V)null);
    }

    /**
     * Update the value for the given key.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @param value the new value (not null)
     * @return the old value
     * @throws MVStoreException if a lock timeout occurs
     */
    @Override
    public V put(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return set(key, value);
    }

    /**
     * Put the value for the given key if entry for this key does not exist.
     * It is atomic equivalent of the following expression:
     * contains(key) ? get(k) : put(key, value);
     *
     * @param key the key
     * @param value the new value (not null)
     * @return the old value
     */
    @Override
    public V putIfAbsent(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        ifAbsentDecisionMaker.initialize(key, value);
        V result = set(key, ifAbsentDecisionMaker, -1);
        if (ifAbsentDecisionMaker.getDecision() == MVMap.Decision.ABORT) {
            result = ifAbsentDecisionMaker.getLastValue();
        }
        return result;
    }

    /**
     * Appends entry to underlying map. This method may be used concurrently,
     * but latest appended values are not guaranteed to be visible.
     * @param key should be higher in map's order than any existing key
     * @param value to be appended
     */
    public void append(K key, V value) {
        map.append(key, VersionedValueUncommitted.getInstance(
                                        transaction.log(new Record<>(map.getId(), key, null)), value, null));
        hasChanges = true;
    }

    /**
     * Lock row for the given key.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @return the locked value
     * @throws MVStoreException if a lock timeout occurs
     */
    public V lock(K key) {
        return lock(key, -1);
    }

    /**
     * Lock row for the given key.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @param timeoutMillis
     *            timeout in milliseconds, {@code -1} for default, {@code -2} to
     *            skip locking if row is already locked by another transaction
     * @return the locked value
     * @throws MVStoreException if a lock timeout occurs
     */
    public V lock(K key, int timeoutMillis) {
        lockDecisionMaker.initialize(key, null);
        return set(key, lockDecisionMaker, timeoutMillis);
    }

    /**
     * Update the value for the given key, without adding an undo log entry.
     *
     * @param key the key
     * @param value the value
     * @return the old value
     */
    @SuppressWarnings("UnusedReturnValue")
    public V putCommitted(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        VersionedValue<V> newValue = VersionedValueCommitted.getInstance(value);
        VersionedValue<V> oldValue = map.put(key, newValue);
        V result = oldValue == null ? null : oldValue.getCurrentValue();
        return result;
    }

    private V set(K key, V value) {
        txDecisionMaker.initialize(key, value);
        return set(key, txDecisionMaker, -1);
    }

    private V set(Object key, TxDecisionMaker<K,V> decisionMaker, int timeoutMillis) {
        Transaction blockingTransaction;
        VersionedValue<V> result;
        String mapName = null;
        do {
            assert transaction.getBlockerId() == 0;
            @SuppressWarnings("unchecked")
            K k = (K) key;
            // second parameter (value) is not really used,
            // since TxDecisionMaker has it embedded
            result = map.operate(k, null, decisionMaker);

            MVMap.Decision decision = decisionMaker.getDecision();
            assert decision != null;
            assert decision != MVMap.Decision.REPEAT;
            blockingTransaction = decisionMaker.getBlockingTransaction();
            if (decision != MVMap.Decision.ABORT || blockingTransaction == null) {
                hasChanges |= decision != MVMap.Decision.ABORT;
                V res = result == null ? null : result.getCurrentValue();
                return res;
            }
            decisionMaker.reset();
            if (timeoutMillis == -2) {
                return null;
            }
            if (mapName == null) {
                mapName = map.getName();
            }
        } while (timeoutMillis != 0 && transaction.waitFor(blockingTransaction, mapName, key, timeoutMillis));

        throw DataUtils.newMVStoreException(DataUtils.ERROR_TRANSACTION_LOCKED,
                "Map entry <{0}> with key <{1}> and value {2} is locked by tx {3} and can not be updated by tx {4}"
                        + " within allocated time interval {5} ms.",
                mapName, key, result, blockingTransaction.transactionId, transaction.transactionId,
                timeoutMillis == -1 ? transaction.timeoutMillis : timeoutMillis);
    }

    /**
     * Try to remove the value for the given key.
     * <p>
     * This will fail if the row is locked by another transaction (that
     * means, if another open transaction changed the row).
     *
     * @param key the key
     * @return whether the entry could be removed
     */
    public boolean tryRemove(K key) {
        return trySet(key, null);
    }

    /**
     * Try to update the value for the given key.
     * <p>
     * This will fail if the row is locked by another transaction (that
     * means, if another open transaction changed the row).
     *
     * @param key the key
     * @param value the new value
     * @return whether the entry could be updated
     */
    public boolean tryPut(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return trySet(key, value);
    }

    /**
     * Try to set or remove the value. When updating only unchanged entries,
     * then the value is only changed if it was not changed after opening
     * the map.
     *
     * @param key the key
     * @param value the new value (null to remove the value)
     * @return true if the value was set, false if there was a concurrent
     *         update
     */
    public boolean trySet(K key, V value) {
        try {
            // TODO: effective transaction.timeoutMillis should be set to 0 here
            // and restored before return
            // TODO: eliminate exception usage as part of normal control flaw
            set(key, value);
            return true;
        } catch (MVStoreException e) {
            return false;
        }
    }

    /**
     * Get the effective value for the given key.
     *
     * @param key the key
     * @return the value or null
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        return getImmediate((K)key);
    }

    /**
     * Get the value for the given key, or null if value does not exist in accordance with transactional rules.
     * Value is taken from a snapshot, appropriate for an isolation level of the related transaction
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V getFromSnapshot(K key) {
        switch (transaction.isolationLevel) {
        case READ_UNCOMMITTED: {
            Snapshot<K,VersionedValue<V>> snapshot = getStatementSnapshot();
            VersionedValue<V> data = map.get(snapshot.root.root, key);
            if (data != null) {
                return data.getCurrentValue();
            }
            return null;
        }
        case REPEATABLE_READ:
        case SNAPSHOT:
        case SERIALIZABLE:
            if (transaction.hasChanges()) {
                Snapshot<K,VersionedValue<V>> snapshot = getStatementSnapshot();
                VersionedValue<V> data = map.get(snapshot.root.root, key);
                if (data != null) {
                    long id = data.getOperationId();
                    if (id != 0L && transaction.transactionId == TransactionStore.getTransactionId(id)) {
                        return data.getCurrentValue();
                    }
                }
            }
            //$FALL-THROUGH$
        case READ_COMMITTED:
        default:
            Snapshot<K,VersionedValue<V>> snapshot = getSnapshot();
            return getFromSnapshot(snapshot.root, snapshot.committingTransactions, key);
        }
    }

    private V getFromSnapshot(RootReference<K, VersionedValue<V>> rootRef, BitSet committingTransactions, K key) {
        VersionedValue<V> data = map.get(rootRef.root, key);
        if (data == null) {
            // doesn't exist
            return null;
        }
        long id = data.getOperationId();
        if (id != 0) {
            int tx = TransactionStore.getTransactionId(id);
            if (tx != transaction.transactionId && !committingTransactions.get(tx)) {
                // added/modified/removed by uncommitted transaction, change should not be visible
                return data.getCommittedValue();
            }
        }
        // added/modified/removed by this transaction or another transaction which is committed by now
        return data.getCurrentValue();
    }

    /**
     * Get the value for the given key, or null if not found.
     * Operation is performed on a snapshot of the map taken during this call.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V getImmediate(K key) {
        return useSnapshot((rootReference, committedTransactions) ->
                                getFromSnapshot(rootReference, committedTransactions, key));
    }

    Snapshot<K,VersionedValue<V>> getSnapshot() {
        return snapshot == null ? createSnapshot() : snapshot;
    }

    Snapshot<K,VersionedValue<V>> getStatementSnapshot() {
        return statementSnapshot == null ? createSnapshot() : statementSnapshot;
    }

    void setStatementSnapshot(Snapshot<K,VersionedValue<V>> snapshot) {
        statementSnapshot = snapshot;
    }

    void promoteSnapshot() {
        if (snapshot == null) {
            snapshot = statementSnapshot;
        }
    }

    /**
     * Create a new snapshot for this map.
     *
     * @return the snapshot
     */
    Snapshot<K,VersionedValue<V>> createSnapshot() {
        return useSnapshot(Snapshot::new);
    }

    /**
     * Gets a coherent picture of committing transactions and root reference,
     * passes it to the specified function, and returns its result.
     *
     * @param <R> type of the result
     *
     * @param snapshotConsumer
     *            function to invoke on a snapshot
     * @return function's result
     */
    <R> R useSnapshot(BiFunction<RootReference<K,VersionedValue<V>>, BitSet, R> snapshotConsumer) {
        // The purpose of the following loop is to get a coherent picture
        // of a state of two independent volatile / atomic variables,
        // which they had at some recent moment in time.
        // In order to get such a "snapshot", we wait for a moment of silence,
        // when neither of the variables concurrently changes it's value.
        AtomicReference<BitSet> holder = transaction.store.committingTransactions;
        BitSet committingTransactions = holder.get();
        while (true) {
            BitSet prevCommittingTransactions = committingTransactions;
            RootReference<K,VersionedValue<V>> root = map.getRoot();
            committingTransactions = holder.get();
            if (committingTransactions == prevCommittingTransactions) {
                return snapshotConsumer.apply(root, committingTransactions);
            }
        }
    }

    /**
     * Whether the map contains the key.
     *
     * @param key the key
     * @return true if the map contains an entry for this key
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object key) {
        return getImmediate((K)key) != null;
    }

    /**
     * Check if the row was deleted by this transaction.
     *
     * @param key the key
     * @return {@code true} if it was
     */
    public boolean isDeletedByCurrentTransaction(K key) {
        VersionedValue<V> data = map.get(key);
        if (data != null) {
            long id = data.getOperationId();
            return id != 0 && TransactionStore.getTransactionId(id) == transaction.transactionId
                    && data.getCurrentValue() == null;
        }
        return false;
    }

    /**
     * Whether the entry for this key was added or removed from this
     * session.
     *
     * @param key the key
     * @return true if yes
     */
    public boolean isSameTransaction(K key) {
        VersionedValue<V> data = map.get(key);
        if (data == null) {
            // doesn't exist or deleted by a committed transaction
            return false;
        }
        int tx = TransactionStore.getTransactionId(data.getOperationId());
        return tx == transaction.transactionId;
    }

    /**
     * Check whether this map is closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return map.isClosed();
    }

    /**
     * Clear the map.
     */
    @Override
    public void clear() {
        // TODO truncate transactionally?
        map.clear();
        hasChanges = true;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {

            @Override
            public Iterator<Entry<K, V>> iterator() {
                return entryIterator(null, null);
            }

            @Override
            public int size() {
                return TransactionMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return TransactionMap.this.containsKey(o);
            }

        };
    }

    /**
     * Get the first entry.
     *
     * @return the first entry, or null if empty
     */
    public Entry<K,V> firstEntry() {
        return this.<Entry<K,V>>chooseIterator(null, null, false, true).fetchNext();
    }

    /**
     * Get the first key.
     *
     * @return the first key, or null if empty
     */
    public K firstKey() {
        return this.<K>chooseIterator(null, null, false, false).fetchNext();
    }

    /**
     * Get the last entry.
     *
     * @return the last entry, or null if empty
     */
    public Entry<K,V> lastEntry() {
        return this.<Entry<K,V>>chooseIterator(null, null, true, true).fetchNext();
    }

    /**
     * Get the last key.
     *
     * @return the last key, or null if empty
     */
    public K lastKey() {
        return this.<K>chooseIterator(null, null, true, false).fetchNext();
    }

    /**
     * Get the entry with smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public Entry<K,V> higherEntry(K key) {
        return higherLowerEntry(key, false);
    }

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K higherKey(K key) {
        return higherLowerKey(key, false);
    }

    /**
     * Get the entry with smallest key that is larger than or equal to this key,
     * or null if no such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public Entry<K,V> ceilingEntry(K key) {
        return this.<Entry<K, V>>chooseIterator(key, null, false, true).fetchNext();
    }

    /**
     * Get the smallest key that is larger than or equal to this key,
     * or null if no such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K ceilingKey(K key) {
        return this.<K>chooseIterator(key, null, false, false).fetchNext();
    }

    /**
     * Get the entry with largest key that is smaller than or equal to this key,
     * or null if no such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public Entry<K,V> floorEntry(K key) {
        return this.<Entry<K, V>>chooseIterator(key, null, true, true).fetchNext();
    }

    /**
     * Get the largest key that is smaller than or equal to this key,
     * or null if no such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K floorKey(K key) {
        return this.<K>chooseIterator(key, null, true, false).fetchNext();
    }

    /**
     * Get the entry with largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public Entry<K,V> lowerEntry(K key) {
        return higherLowerEntry(key, true);
    }

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K lowerKey(K key) {
        return higherLowerKey(key, true);
    }

    private Entry<K, V> higherLowerEntry(K key, boolean lower) {
        TMIterator<K, V, Entry<K, V>> it = chooseIterator(key, null, lower, true);
        Entry<K, V> result = it.fetchNext();
        if (result != null && map.getKeyType().compare(key, result.getKey()) == 0) {
            result = it.fetchNext();
        }
        return result;
    }

    private K higherLowerKey(K key, boolean lower) {
        TMIterator<K, V, K> it = chooseIterator(key, null, lower, false);
        K result = it.fetchNext();
        if (result != null && map.getKeyType().compare(key, result) == 0) {
            result = it.fetchNext();
        }
        return result;
    }

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from) {
        return chooseIterator(from, null, false, false);
    }

    /**
     * Iterate over keys in the specified order.
     *
     * @param from the first key to return
     * @param reverse if true, iterate in reverse (descending) order
     * @return the iterator
     */
    public TMIterator<K, V, K> keyIterator(K from, boolean reverse) {
        return chooseIterator(from, null, reverse, false);
    }

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @param to the last key to return or null if there is no limit
     * @return the iterator
     */
    public TMIterator<K, V, K> keyIterator(K from, K to) {
        return chooseIterator(from, to, false, false);
    }

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @param to the last key to return or null if there is no limit
     * @param reverse if true, iterate in reverse (descending) order
     * @return the iterator
     */
    public TMIterator<K, V, K> keyIterator(K from, K to, boolean reverse) {
        return chooseIterator(from, to, reverse, false);
    }

    /**
     * Iterate over keys, including keys from uncommitted entries.
     *
     * @param from the first key to return
     * @param to the last key to return or null if there is no limit
     * @return the iterator
     */
    public TMIterator<K, V, K> keyIteratorUncommitted(K from, K to) {
        return new ValidationIterator<>(this, from, to);
    }

    /**
     * Iterate over entries.
     *
     * @param from the first key to return
     * @param to the last key to return
     * @return the iterator
     */
    public TMIterator<K, V, Map.Entry<K, V>> entryIterator(final K from, final K to) {
        return chooseIterator(from, to, false, true);
    }

    /**
     * Iterate over entries.
     *
     * @param from the first key to return
     * @param to the last key to return
     * @param reverse if true, iterate in reverse (descending) order
     * @return the iterator
     */
    public TMIterator<K, V, Map.Entry<K, V>> entryIterator(K from, K to, boolean reverse) {
        return chooseIterator(from, to, reverse, true);
    }

    private <X> TMIterator<K, V, X> chooseIterator(K from, K to, boolean reverse, boolean forEntries) {
        switch (transaction.isolationLevel) {
            case READ_UNCOMMITTED:
                return new UncommittedIterator<>(this, from, to, reverse, forEntries);
            case REPEATABLE_READ:
            case SNAPSHOT:
            case SERIALIZABLE:
                if (hasChanges) {
                    return new RepeatableIterator<>(this, from, to, reverse, forEntries);
                }
                //$FALL-THROUGH$
            case READ_COMMITTED:
            default:
                return new CommittedIterator<>(this, from, to, reverse, forEntries);
        }
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public DataType<K> getKeyType() {
        return map.getKeyType();
    }

    /**
     * The iterator for read uncommitted isolation level. This iterator is also
     * used for unique indexes.
     *
     * @param <K>
     *            the type of keys
     * @param <X>
     *            the type of elements
     */
    private static class UncommittedIterator<K,V,X> extends TMIterator<K,V,X> {
        UncommittedIterator(TransactionMap<K, V> transactionMap, K from, K to, boolean reverse, boolean forEntries) {
            super(transactionMap, from, to, transactionMap.createSnapshot(), reverse, forEntries);
        }

        UncommittedIterator(TransactionMap<K, V> transactionMap, K from, K to, Snapshot<K, VersionedValue<V>> snapshot,
                            boolean reverse, boolean forEntries) {
            super(transactionMap, from, to, snapshot, reverse, forEntries);
        }

        @Override
        public final X fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue<?> data = cursor.getValue();
                if (data != null) {
                    Object currentValue = data.getCurrentValue();
                    if (currentValue != null || shouldIgnoreRemoval(data)) {
                        return toElement(key, currentValue);
                    }
                }
            }
            return null;
        }

        boolean shouldIgnoreRemoval(VersionedValue<?> data) {
            return false;
        }
    }


    // This iterator should include all entries applicable for unique index validation,
    // committed and otherwise, only excluding keys removed by the current transaction
    // or by some other already committed (but not closed yet) transactions
    private static final class ValidationIterator<K,V,X> extends UncommittedIterator<K,V,X> {
        ValidationIterator(TransactionMap<K,V> transactionMap, K from, K to) {
            super(transactionMap, from, to, transactionMap.createSnapshot(), false, false);
        }

        @Override
        boolean shouldIgnoreRemoval(VersionedValue<?> data) {
            assert data.getCurrentValue() == null;
            long id = data.getOperationId();
            if (id != 0) {
                int tx = TransactionStore.getTransactionId(id);
                return transactionId != tx && !committingTransactions.get(tx);
            }
            return false;
        }
    }

    /**
     * The iterator for read committed isolation level. Can also be used on
     * higher levels when the transaction doesn't have own changes.
     *
     * @param <K>
     *            the type of keys
     * @param <X>
     *            the type of elements
     */
    private static final class CommittedIterator<K,V,X> extends TMIterator<K,V,X> {
        CommittedIterator(TransactionMap<K, V> transactionMap, K from, K to, boolean reverse, boolean forEntries) {
            super(transactionMap, from, to, transactionMap.getSnapshot(), reverse, forEntries);
        }

        @Override
        public X fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue<?> data = cursor.getValue();
                // If value doesn't exist or it was deleted by a committed transaction,
                // or if value is a committed one, just return it.
                if (data != null) {
                    long id = data.getOperationId();
                    if (id != 0) {
                        int tx = TransactionStore.getTransactionId(id);
                        if (tx != transactionId && !committingTransactions.get(tx)) {
                            // current value comes from another uncommitted transaction
                            // take committed value instead
                            Object committedValue = data.getCommittedValue();
                            if (committedValue == null) {
                                continue;
                            }
                            return toElement(key, committedValue);
                        }
                    }
                    Object currentValue = data.getCurrentValue();
                    if (currentValue != null) {
                        return toElement(key, currentValue);
                    }
                }
            }
            return null;
        }
    }

    /**
     * The iterator for repeatable read and serializable isolation levels.
     *
     * @param <K>
     *            the type of keys
     * @param <X>
     *            the type of elements
     */
    private static final class RepeatableIterator<K,V,X> extends TMIterator<K,V,X> {
        private final DataType<K> keyType;

        private K snapshotKey;

        private Object snapshotValue;

        private final Cursor<K, VersionedValue<V>> uncommittedCursor;

        private K uncommittedKey;

        private V uncommittedValue;

        RepeatableIterator(TransactionMap<K, V> transactionMap, K from, K to, boolean reverse, boolean forEntries) {
            super(transactionMap, from, to, transactionMap.getSnapshot(), reverse, forEntries);
            keyType = transactionMap.map.getKeyType();
            Snapshot<K,VersionedValue<V>> snapshot = transactionMap.getStatementSnapshot();
            uncommittedCursor = transactionMap.map.cursor(snapshot.root, from, to, reverse);
        }

        @Override
        public X fetchNext() {
            X next = null;
            do {
                if (snapshotKey == null) {
                    fetchSnapshot();
                }
                if (uncommittedKey == null) {
                    fetchUncommitted();
                }
                if (snapshotKey == null && uncommittedKey == null) {
                    break;
                }
                int cmp = snapshotKey == null ? 1 :
                            uncommittedKey == null ? -1 :
                            keyType.compare(snapshotKey, uncommittedKey);
                if (cmp < 0) {
                    next = toElement(snapshotKey, snapshotValue);
                    snapshotKey = null;
                    break;
                }
                if (uncommittedValue != null) {
                    // This entry was added / updated by this transaction, use the new value
                    next = toElement(uncommittedKey, uncommittedValue);
                }
                if (cmp == 0) { // This entry was updated / deleted
                    snapshotKey = null;
                }
                uncommittedKey = null;
            } while (next == null);
            return next;
        }

        private void fetchSnapshot() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue<?> data = cursor.getValue();
                // If value doesn't exist or it was deleted by a committed transaction,
                // or if value is a committed one, just return it.
                if (data != null) {
                    Object value = data.getCommittedValue();
                    long id = data.getOperationId();
                    if (id != 0) {
                        int tx = TransactionStore.getTransactionId(id);
                        if (tx == transactionId || committingTransactions.get(tx)) {
                            // value comes from this transaction or another committed transaction
                            // take current value instead instead of committed one
                            value = data.getCurrentValue();
                        }
                    }
                    if (value != null) {
                        snapshotKey = key;
                        snapshotValue = value;
                        return;
                    }
                }
            }
        }

        private void fetchUncommitted() {
            while (uncommittedCursor.hasNext()) {
                K key = uncommittedCursor.next();
                VersionedValue<V> data = uncommittedCursor.getValue();
                if (data != null) {
                    long id = data.getOperationId();
                    if (id != 0L && transactionId == TransactionStore.getTransactionId(id)) {
                        uncommittedKey = key;
                        uncommittedValue = data.getCurrentValue();
                        return;
                    }
                }
            }
        }
    }

    public abstract static class TMIterator<K,V,X> implements Iterator<X> {
        final int transactionId;

        final BitSet committingTransactions;

        protected final Cursor<K, VersionedValue<V>> cursor;

        private final boolean forEntries;

        X current;

        TMIterator(TransactionMap<K, V> transactionMap, K from, K to, Snapshot<K, VersionedValue<V>> snapshot,
                boolean reverse, boolean forEntries) {
            Transaction transaction = transactionMap.getTransaction();
            this.transactionId = transaction.transactionId;
            this.forEntries = forEntries;
            this.cursor = transactionMap.map.cursor(snapshot.root, from, to, reverse);
            this.committingTransactions = snapshot.committingTransactions;
        }

        @SuppressWarnings("unchecked")
        final X toElement(K key, Object value) {
            return (X) (forEntries ? new AbstractMap.SimpleImmutableEntry<>(key, value) : key);
        }

        /**
         * Fetches a next entry.
         *
         * This method cannot be used together with {@link #hasNext()} and
         * {@link #next()}.
         *
         * @return the next entry or {@code null}
         */
        public abstract X fetchNext();

        @Override
        public final boolean hasNext() {
            return current != null || (current = fetchNext()) != null;
        }

        @Override
        public final X next() {
            X result = current;
            if (result == null) {
                if ((result = fetchNext()) == null) {
                    throw new NoSuchElementException();
                }
            } else {
                current = null;
            }
            return result;
        }

    }

}
