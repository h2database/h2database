/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.mvstore.RootReference;
import org.h2.mvstore.type.DataType;
import org.h2.value.VersionedValue;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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
public class TransactionMap<K, V> extends AbstractMap<K, V> {

    /**
     * The map used for writing (the latest version).
     * <p>
     * Key: key the key of the data.
     * Value: { transactionId, oldVersion, value }
     */
    public final MVMap<K, VersionedValue> map;

    /**
     * The transaction which is used for this map.
     */
    private final Transaction transaction;

    TransactionMap(Transaction transaction, MVMap<K, VersionedValue> map) {
        this.transaction = transaction;
        this.map = map;
    }

    /**
     * Get a clone of this map for the given transaction.
     *
     * @param transaction the transaction
     * @return the map
     */
    public TransactionMap<K, V> getInstance(Transaction transaction) {
        return new TransactionMap<>(transaction, map);
    }

    /**
     * Get the number of entries, as a integer. {@link Integer#MAX_VALUE} is
     * returned if there are more than this entries.
     *
     * @return the number of entries, as an integer
     * @see #sizeAsLong()
     */
    @Override
    public final int size() {
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
        if (!transaction.isolationLevel.allowNonRepeatableRead()) {
            return sizeAsLongSlow();
        }
        // getting coherent picture of the map, committing transactions, and undo logs
        // either from values stored in transaction (never loops in that case),
        // or current values from the transaction store (loops until moment of silence)
        BitSet committingTransactions;
        RootReference mapRootReference;
        RootReference[] undoLogRootReferences;
        do {
            committingTransactions = getCommittingTransactions();
            mapRootReference = getRootReference();
            undoLogRootReferences = getTransaction().getUndoLogRootReferences();
        } while(committingTransactions != getCommittingTransactions() ||
                mapRootReference != getRootReference());


        Page mapRootPage = mapRootReference.root;
        long size = mapRootReference.getTotalCount();
        long undoLogsTotalSize = undoLogRootReferences == null ? size
                : TransactionStore.calculateUndoLogsTotalSize(undoLogRootReferences);
        // if we are looking at the map without any uncommitted values
        if (undoLogsTotalSize == 0) {
            return size;
        }

        // Entries describing removals from the map by this transaction and all transactions,
        // which are committed but not closed yet,
        // and entries about additions to the map by other uncommitted transactions were counted,
        // but they should not contribute into total count.
        if (2 * undoLogsTotalSize > size) {
            // the undo log is larger than half of the map - scan the entries of the map directly
            Cursor<K, VersionedValue> cursor = new Cursor<>(mapRootPage, null);
            while(cursor.hasNext()) {
                cursor.next();
                VersionedValue currentValue = cursor.getValue();
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
            for (RootReference undoLogRootReference : undoLogRootReferences) {
                if (undoLogRootReference != null) {
                    Cursor<Long, Object[]> cursor = new Cursor<>(undoLogRootReference.root, null);
                    while (cursor.hasNext()) {
                        cursor.next();
                        Object[] op = cursor.getValue();
                        if ((int) op[0] == map.getId()) {
                            VersionedValue currentValue = map.get(mapRootPage, op[1]);
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

    private long sizeAsLongSlow() {
        long count = 0L;
        TMIterator<K, ?> iterator = getRepeatableIterator(null, null, false);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private boolean isIrrelevant(long operationId, VersionedValue currentValue, BitSet committingTransactions) {
        int txId = TransactionStore.getTransactionId(operationId);
        boolean isVisible = txId == transaction.transactionId || committingTransactions.get(txId);
        Object v = isVisible ? currentValue.getCurrentValue() : currentValue.getCommittedValue();
        return v == null;
    }


    /**
     * Remove an entry.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @throws IllegalStateException if a lock timeout occurs
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @Override
    public V remove(Object key) {
        return set(key, (V)null);
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
     * @throws IllegalStateException if a lock timeout occurs
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
    // Do not add @Override, code should be compatible with Java 7
    public V putIfAbsent(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        TxDecisionMaker decisionMaker = new TxDecisionMaker.PutIfAbsentDecisionMaker(map.getId(), key, value,
                transaction);
        return set(key, decisionMaker);
    }

    /**
     * Appends entry to underlying map. This method may be used concurrently,
     * but latest appended values are not guaranteed to be visible.
     * @param key should be higher in map's order than any existing key
     * @param value to be appended
     */
    public void append(K key, V value) {
        map.append(key, VersionedValueUncommitted.getInstance(transaction.log(map.getId(), key, null), value, null));
    }

    /**
     * Lock row for the given key.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @return the locked value
     * @throws IllegalStateException if a lock timeout occurs
     */
    public V lock(K key) {
        TxDecisionMaker decisionMaker = new TxDecisionMaker.LockDecisionMaker(map.getId(), key, transaction);
        return set(key, decisionMaker);
    }

    /**
     * Update the value for the given key, without adding an undo log entry.
     *
     * @param key the key
     * @param value the value
     * @return the old value
     */
    public V putCommitted(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        VersionedValue newValue = VersionedValueCommitted.getInstance(value);
        VersionedValue oldValue = map.put(key, newValue);
        @SuppressWarnings("unchecked")
        V result = (V) (oldValue == null ? null : oldValue.getCurrentValue());
        return result;
    }

    private V set(Object key, V value) {
        TxDecisionMaker decisionMaker = new TxDecisionMaker(map.getId(), key, value, transaction);
        return set(key, decisionMaker);
    }

    private V set(Object key, TxDecisionMaker decisionMaker) {
        TransactionStore store = transaction.store;
        Transaction blockingTransaction;
        long sequenceNumWhenStarted;
        VersionedValue result;
        do {
            sequenceNumWhenStarted = store.openTransactions.get().getVersion();
            assert transaction.getBlockerId() == 0;
            // although second parameter (value) is not really used,
            // since TxDecisionMaker has it embedded,
            // MVRTreeMap has weird traversal logic based on it,
            // and any non-null value will do
            @SuppressWarnings("unchecked")
            K k = (K) key;
            result = map.operate(k, VersionedValue.DUMMY, decisionMaker);

            MVMap.Decision decision = decisionMaker.getDecision();
            assert decision != null;
            assert decision != MVMap.Decision.REPEAT;
            blockingTransaction = decisionMaker.getBlockingTransaction();
            if (decision != MVMap.Decision.ABORT || blockingTransaction == null) {
                @SuppressWarnings("unchecked")
                V res = result == null ? null : (V) result.getCurrentValue();
                return res;
            }
            decisionMaker.reset();
        } while (blockingTransaction.sequenceNum > sequenceNumWhenStarted
                || transaction.waitFor(blockingTransaction, map, key));

        throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED,
                "Map entry <{0}> with key <{1}> and value {2} is locked by tx {3} and can not be updated by tx {4}"
                        + " within allocated time interval {5} ms.",
                map.getName(), key, result, blockingTransaction.transactionId, transaction.transactionId,
                transaction.timeoutMillis);
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
        } catch (IllegalStateException e) {
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
    @Override
    public V get(Object key) {
        return getImmediate(key);
    }

    /**
     * Get the value for the given key from a snapshot, or null if not found.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public V getFromSnapshot(Object key) {
        RootReference rootReference = getRootReference();
        VersionedValue data = map.get(rootReference.root, key);
        if (data == null) {
            // doesn't exist or deleted by a committed transaction
            return null;
        }
        long id = data.getOperationId();
        if (id == 0) {
            // it is committed
            return (V)data.getCurrentValue();
        }
        int tx = TransactionStore.getTransactionId(id);
        if (tx == transaction.transactionId || getCommittingTransactions().get(tx)) {
            // added by this transaction or another transaction which is committed by now
            return (V) data.getCurrentValue();
        } else {
            return (V) data.getCommittedValue();
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public V getImmediate(Object key) {
        VersionedValue data = map.get(key);
        if (data == null) {
            // doesn't exist or deleted by a committed transaction
            return null;
        }
        long id = data.getOperationId();
        if (id == 0) {
            // it is committed
            return (V)data.getCurrentValue();
        }
        int tx = TransactionStore.getTransactionId(id);
        if (tx == transaction.transactionId || transaction.store.committingTransactions.get().get(tx)) {
            // added by this transaction or another transaction which is committed by now
            return (V) data.getCurrentValue();
        } else {
            return (V) data.getCommittedValue();
        }
    }

    BitSet getCommittingTransactions() {
        return transaction.getCommittingTransactions();
    }

    RootReference getRootReference() {
        return transaction.getMapRoot(map.getId());
    }

    /**
     * Whether the map contains the key.
     *
     * @param key the key
     * @return true if the map contains an entry for this key
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @Override
    public boolean containsKey(Object key) {
        return getImmediate(key) != null;
    }

    /**
     * Whether the entry for this key was added or removed from this
     * session.
     *
     * @param key the key
     * @return true if yes
     */
    public boolean isSameTransaction(K key) {
        VersionedValue data = map.get(key);
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
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<Entry<K, V>>() {

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
     * Get the first key.
     *
     * @return the first key, or null if empty
     */
    public K firstKey() {
        Iterator<K> it = keyIterator(null);
        return it.hasNext() ? it.next() : null;
    }

    /**
     * Get the last key.
     *
     * @return the last key, or null if empty
     */
    public K lastKey() {
        RootReference rootReference = getRootReference();
        K k = map.lastKey(rootReference.root);
        while (k != null && getFromSnapshot(k) == null) {
            k = map.lowerKey(rootReference.root, k);
        }
        return k;
    }

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K higherKey(K key) {
        RootReference rootReference = getRootReference();
        do {
            key = map.higherKey(rootReference.root, key);
        } while (key != null && getFromSnapshot(key) == null);
        return key;
    }

    /**
     * Get the smallest key that is larger than or equal to this key,
     * or null if no such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K ceilingKey(K key) {
        Iterator<K> it = keyIterator(key);
        return it.hasNext() ? it.next() : null;
    }

    /**
     * Get the largest key that is smaller than or equal to this key,
     * or null if no such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K floorKey(K key) {
        RootReference rootReference = getRootReference();
        key = map.floorKey(rootReference.root, key);
        while (key != null && getFromSnapshot(key) == null) {
            // Use lowerKey() for the next attempts, otherwise we'll get an infinite loop
            key = map.lowerKey(rootReference.root, key);
        }
        return key;
    }

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K lowerKey(K key) {
        RootReference rootReference = getRootReference();
        do {
            key = map.lowerKey(rootReference.root, key);
        } while (key != null && getFromSnapshot(key) == null);
        return key;
    }

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from) {
        return keyIterator(from, null, false);
    }

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @param to the last key to return or null if there is no limit
     * @param includeUncommitted whether uncommitted entries should be
     *            included
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from, K to, boolean includeUncommitted) {
        if (includeUncommitted) {
            return new UncommittedIterator<>(this, from, to, false);
        }
        return transaction.isolationLevel.allowNonRepeatableRead() ? new CommittedIterator<K, K>(this, from, to, false)
                : this.<K> getRepeatableIterator(from, to, false);
    }

    /**
     * Iterate over entries.
     *
     * @param from the first key to return
     * @param to the last key to return
     * @return the iterator
     */
    public Iterator<Map.Entry<K, V>> entryIterator(final K from, final K to) {
        return transaction.isolationLevel.allowNonRepeatableRead()
                ? new CommittedIterator<K, Entry<K, V>>(this, from, to, true)
                : this.<Entry<K, V>> getRepeatableIterator(from, to, true);
    }

    /**
     * Iterate over keys.
     *
     * @param iterator the iterator to wrap
     * @param includeUncommitted whether uncommitted entries should be
     *            included
     * @return the iterator
     */
    public Iterator<K> wrapIterator(final Iterator<K> iterator,
            final boolean includeUncommitted) {
        // TODO duplicate code for wrapIterator and entryIterator
        return new Iterator<K>() {
            private K current;

            {
                fetchNext();
            }

            private void fetchNext() {
                while (iterator.hasNext()) {
                    current = iterator.next();
                    if (includeUncommitted) {
                        return;
                    }
                    if (containsKey(current)) {
                        return;
                    }
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public K next() {
                K result = current;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException(
                        "Removal is not supported");
            }
        };
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public DataType getKeyType() {
        return map.getKeyType();
    }

    private <X> TMIterator<K, X> getRepeatableIterator(K from, K to, boolean forEntries) {
        return transaction.hasChanges() ? new RepeatableIterator<K, X>(this, from, to, forEntries)
                : new CommittedIterator<K, X>(this, from, to, forEntries);
    }

    private static final class UncommittedIterator<K, X> extends TMIterator<K, X> {

        private final int transactionId;

        private final Cursor<K, VersionedValue> cursor;

        UncommittedIterator(TransactionMap<K, ?> transactionMap, K from, K to, boolean forEntries) {
            super(forEntries);
            Transaction transaction = transactionMap.getTransaction();
            this.transactionId = transaction.transactionId;
            this.cursor = new Cursor<>(transactionMap.map.flushAndGetRoot().root, from, to);
            fetchNext();
        }

        @Override
        void fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue data = cursor.getValue();
                if (data != null) {
                    Object currentValue = data.getCurrentValue();
                    if (currentValue != null
                            || transactionId != TransactionStore.getTransactionId(data.getOperationId())) {
                        registerCurrent(key, currentValue);
                        return;
                    }
                }
            }
            current = null;
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
    private static final class CommittedIterator<K, X> extends TMIterator<K, X> {

        private final int transactionId;

        private final BitSet committingTransactions;

        private final Cursor<K, VersionedValue> cursor;

        CommittedIterator(TransactionMap<K, ?> transactionMap, K from, K to, boolean forEntries) {
            super(forEntries);
            Transaction transaction = transactionMap.getTransaction();
            this.transactionId = transaction.transactionId;
            // The purpose of the following loop is to get a coherent picture
            // of a state of two independent volatile / atomic variables,
            // which they had at some recent moment in time.
            // In order to get such a "snapshot", we wait for a moment of silence,
            // when neither of the variables concurrently changes it's value.
            BitSet committingTransactions;
            RootReference mapRootReference;
            do {
                committingTransactions = transactionMap.getCommittingTransactions();
                mapRootReference = transactionMap.getRootReference();
            } while (committingTransactions != transactionMap.getCommittingTransactions());
            // Now we have a snapshot, where mapRootReference points to state of the map
            // and committingTransactions mask tells us which of seemingly uncommitted changes
            // should be considered as committed.
            // Subsequent map traversal uses this snapshot info only.
            this.cursor = new Cursor<>(mapRootReference.root, from, to);
            this.committingTransactions = committingTransactions;
            fetchNext();
        }

        @Override
        void fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue data = cursor.getValue();
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
                            registerCurrent(key, committedValue);
                            return;
                        }
                    }
                    Object currentValue = data.getCurrentValue();
                    if (currentValue != null) {
                        registerCurrent(key, currentValue);
                        return;
                    }
                }
            }
            current = null;
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
    private static final class RepeatableIterator<K, X> extends TMIterator<K, X> {

        private final DataType keyType;

        private final int transactionId;

        private final BitSet snapshotCommittingTransactions;

        private final Cursor<K, VersionedValue> snapshotCursor;

        private K snapshotKey;

        private Object snapshotValue;

        private final Cursor<K, VersionedValue> uncommittedCursor;

        private K uncommittedKey;

        private Object uncommittedValue;

        RepeatableIterator(TransactionMap<K, ?> transactionMap, K from, K to, boolean forEntries) {
            super(forEntries);
            keyType = transactionMap.map.getKeyType();
            Transaction transaction = transactionMap.getTransaction();
            this.transactionId = transaction.transactionId;
            // The purpose of the following loop is to get a coherent picture
            // of a state of two independent volatile / atomic variables,
            // which they had at some recent moment in time.
            // In order to get such a "snapshot", we wait for a moment of silence,
            // when neither of the variables concurrently changes it's value.
            BitSet committingTransactions;
            RootReference mapRootReference;
            do {
                committingTransactions = transactionMap.getCommittingTransactions();
                mapRootReference = transactionMap.getRootReference();
            } while (committingTransactions != transactionMap.getCommittingTransactions());
            // Now we have a snapshot, where mapRootReference points to state of the map
            // and committingTransactions mask tells us which of seemingly uncommitted changes
            // should be considered as committed.
            // Subsequent map traversal uses this snapshot info only.
            this.snapshotCursor = new Cursor<>(mapRootReference.root, from, to);
            this.snapshotCommittingTransactions = committingTransactions;

            this.uncommittedCursor = new Cursor<>(transactionMap.map.flushAndGetRoot().root, from, to);

            fetchNext();
        }

        @Override
        void fetchNext() {
            for (;;) {
                if (snapshotKey == null) {
                    fetchSnapshot();
                }
                if (uncommittedKey == null) {
                    fetchUncommitted();
                }
                if (snapshotKey != null) {
                    if (uncommittedKey != null) {
                        int cmp = keyType.compare(snapshotKey, uncommittedKey);
                        if (cmp == 0) {
                            if (uncommittedValue == null) {
                                // This entry was deleted by this transaction
                                snapshotKey = null;
                                snapshotValue = null;
                                uncommittedKey = null;
                                continue;
                            }
                            // This entry was updated by this transaction, use the new value
                            registerCurrent(uncommittedKey, uncommittedValue);
                            snapshotKey = null;
                            snapshotValue = null;
                            uncommittedKey = null;
                            uncommittedValue = null;
                        } else if (cmp > 0) {
                            if (uncommittedValue == null) {
                                // Need to fetch the next uncommitted entry
                                uncommittedKey = null;
                                continue;
                            }
                            registerCurrent(uncommittedKey, uncommittedValue);
                            uncommittedKey = null;
                            uncommittedValue = null;
                        } else {
                            registerCurrent(snapshotKey, snapshotValue);
                            snapshotKey = null;
                            snapshotValue = null;
                        }
                    } else {
                        registerCurrent(snapshotKey, snapshotValue);
                        snapshotKey = null;
                        snapshotValue = null;
                    }
                } else {
                    if (uncommittedKey != null) {
                        if (uncommittedValue == null) {
                            // Need to fetch the next uncommitted entry
                            uncommittedKey = null;
                            continue;
                        }
                        registerCurrent(uncommittedKey, uncommittedValue);
                        uncommittedKey = null;
                        uncommittedValue = null;
                    } else {
                        current = null;
                    }
                }
                break;
            }
        }

        private void fetchSnapshot() {
            while (snapshotCursor.hasNext()) {
                K key = snapshotCursor.next();
                VersionedValue data = snapshotCursor.getValue();
                // If value doesn't exist or it was deleted by a committed transaction,
                // or if value is a committed one, just return it.
                if (data != null) {
                    long id = data.getOperationId();
                    if (id != 0) {
                        int tx = TransactionStore.getTransactionId(id);
                        if (tx != transactionId && !snapshotCommittingTransactions.get(tx)) {
                            // current value comes from another uncommitted transaction
                            // take committed value instead
                            Object committedValue = data.getCommittedValue();
                            if (committedValue == null) {
                                continue;
                            }
                            snapshotKey = key;
                            snapshotValue = committedValue;
                            return;
                        }
                    }
                    Object currentValue = data.getCurrentValue();
                    if (currentValue != null) {
                        snapshotKey = key;
                        snapshotValue = currentValue;
                        return;
                    }
                }
            }
        }

        private void fetchUncommitted() {
            while (uncommittedCursor.hasNext()) {
                K key = uncommittedCursor.next();
                VersionedValue data = uncommittedCursor.getValue();
                if (data != null) {
                    if (transactionId == TransactionStore.getTransactionId(data.getOperationId())) {
                        uncommittedKey = key;
                        uncommittedValue = data.getCurrentValue();
                        return;
                    }
                }
            }
        }

    }

    private abstract static class TMIterator<K,X> implements Iterator<X> {

        private final boolean forEntries;

        X current;

        TMIterator(boolean forEntries) {
            this.forEntries = forEntries;
        }

        @SuppressWarnings("unchecked")
        final void registerCurrent(K key, Object value) {
            current = (X) (forEntries ? new AbstractMap.SimpleImmutableEntry<>(key, value) : key);
        }

        abstract void fetchNext();

        @Override
        public final boolean hasNext() {
            return current != null;
        }

        @Override
        public final X next() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            X result = current;
            fetchNext();
            return result;
        }

        @Override
        public final void remove() {
            throw DataUtils.newUnsupportedOperationException(
                    "Removal is not supported");
        }
    }

}
