/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.h2.mvstore.MVMap.Builder;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.New;

/**
 * A store that supports concurrent transactions.
 */
public class TransactionStore {

    private static final String LAST_TRANSACTION_ID = "lastTransactionId";

    /**
     * The store.
     */
    final MVStore store;

    /**
     * The persisted map of open transaction.
     * Key: transactionId, value: [ status, name ].
     */
    final MVMap<Long, Object[]> openTransactions;

    /**
     * The map of open transaction objects.
     * Key: transactionId, value: transaction object.
     */
    final HashMap<Long, Transaction> openTransactionMap = New.hashMap();

    /**
     * The undo log.
     * Key: [ transactionId, logId ], value: [ baseVersion, mapId, key ].
     */
    final MVMap<long[], Object[]> undoLog;

    /**
     * The lock timeout in milliseconds. 0 means timeout immediately.
     */
    long lockTimeout;

    /**
     * The transaction settings. "lastTransaction" the last transaction id.
     */
    private final MVMap<String, String> settings;

    private long lastTransactionIdStored;

    private long lastTransactionId;

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new ObjectDataType());
    }
    
    /**
     * Create a new transaction store.
     *
     * @param store the store
     * @param keyType the data type for map keys
     */
    public TransactionStore(MVStore store, DataType keyType) {
        this.store = store;
        settings = store.openMap("settings");
        openTransactions = store.openMap("openTransactions",
                new MVMapConcurrent.Builder<Long, Object[]>());
        // commit could be faster if we have one undo log per transaction,
        // or a range delete operation for maps
        ArrayType valueType = new ArrayType(new DataType[]{
                new ObjectDataType(), new ObjectDataType(), keyType
        });
        MVMapConcurrent.Builder<long[], Object[]> builder = 
                new MVMapConcurrent.Builder<long[], Object[]>().
                valueType(valueType);
        // TODO escape other map names, to avoid conflicts
        undoLog = store.openMap("undoLog", builder);
        init();
    }

    private void init() {
        String s = settings.get(LAST_TRANSACTION_ID);
        if (s != null) {
            lastTransactionId = Long.parseLong(s);
            lastTransactionIdStored = lastTransactionId;
        }
        Long lastKey = openTransactions.lastKey();
        if (lastKey != null && lastKey.longValue() > lastTransactionId) {
            throw DataUtils.newIllegalStateException("Last transaction not stored");
        }
        Cursor<Long> cursor = openTransactions.keyIterator(null);
        while (cursor.hasNext()) {
            long id = cursor.next();
            Object[] data = openTransactions.get(id);
            int status = (Integer) data[0];
            String name = (String) data[1];
            long[] next = { id + 1, 0 };
            long[] last = undoLog.floorKey(next);
            if (last == null) {
                // no entry
            } else if (last[0] == id) {
                Transaction t = new Transaction(this, id, status, name, last[1]);
                openTransactionMap.put(id, t);
            }
        }
    }

    /**
     * Get the list of currently open transactions that have pending writes.
     * 
     * @return the list of transactions
     */
    public synchronized List<Transaction> getOpenTransactions() {
        ArrayList<Transaction> list = New.arrayList();
        list.addAll(openTransactionMap.values());
        return list;
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        // to avoid losing transaction ids
        settings.put(LAST_TRANSACTION_ID, "" + lastTransactionId);
        store.commit();
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public synchronized Transaction begin() {
        store.incrementVersion();
        long transactionId = lastTransactionId++;
        int status = Transaction.STATUS_OPEN;
        return new Transaction(this, transactionId, status, null, 0);
    }
    
    void storeTransaction(Transaction t) {
        long transactionId = t.getId();
        if (openTransactions.containsKey(transactionId)) {
            return;
        }
        Object[] v = { t.getStatus(), null };
        openTransactions.put(transactionId, v);
        openTransactionMap.put(transactionId, t);
        if (lastTransactionId > lastTransactionIdStored) {
            lastTransactionIdStored += 32;
            settings.put(LAST_TRANSACTION_ID, "" + lastTransactionIdStored);
        }
    }

    /**
     * Prepare a transaction.
     *
     * @param transactionId the transaction id
     */
    void prepare(Transaction t) {
        storeTransaction(t);
        Object[] old = openTransactions.get(t.getId());
        Object[] v = { Transaction.STATUS_PREPARED, old[1] };
        openTransactions.put(t.getId(), v);
        store.commit();
    }
    
    /**
     * Log an entry.
     * 
     * @param t the transaction
     * @param logId the log id
     * @param opType the operation type
     * @param mapId the map id
     * @param key the key
     */
    void log(Transaction t, long logId, int opType, int mapId,
            Object key) {
        storeTransaction(t);
        long[] undoKey = { t.getId(), logId };
        Object[] log = new Object[] { opType, mapId, key };
        undoLog.put(undoKey, log);
    }

    /**
     * Set the name of a transaction.
     *
     * @param t the transaction
     * @param name the new name
     */
    void setTransactionName(Transaction t, String name) {
        storeTransaction(t);
        Object[] old = openTransactions.get(t.getId());
        Object[] v = { old[0], name };
        openTransactions.put(t.getId(), v);
        store.commit();
    }

    /**
     * Commit a transaction.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     */
    void commit(Transaction t, long maxLogId) {
        store.incrementVersion();
        for (long logId = 0; logId < maxLogId; logId++) {
            long[] undoKey = new long[] {
                    t.getId(), logId };
            Object[] op = undoLog.get(undoKey);
            int opType = (Integer) op[0];
            if (opType == Transaction.OP_REMOVE) {
                int mapId = (Integer) op[1];
                Map<String, String> meta = store.getMetaMap();
                String m = meta.get("map." + mapId);
                String mapName = DataUtils.parseMap(m).get("name");
                MVMap<Object, Object[]> map = store.openMap(mapName);
                Object key = op[2];
                Object[] value = map.get(key);
                // possibly the entry was added later on
                // so we have to check
                if (value[2] == null) {
                    // remove the value
                    map.remove(key);
                }
            }
            undoLog.remove(undoKey);
        }
        openTransactions.remove(t.getId());
        openTransactionMap.remove(t.getId());
        store.commit();
        long oldestVersion = store.getCurrentVersion();
        for (Transaction u : openTransactionMap.values()) {
            long v = u.startVersion;
            if (v < oldestVersion) {
                oldestVersion = v;
            }
        }
        store.setRetainVersion(oldestVersion);
    }

    /**
     * Roll a transaction back.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     */
    void rollback(Transaction t, long maxLogId) {
        rollbackTo(t, maxLogId, 0);
        openTransactions.remove(t.getId());
        openTransactionMap.remove(t.getId());
        store.commit();
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        store.incrementVersion();
        for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
            Object[] op = undoLog.get(new long[] {
                    t.getId(), logId });
            int mapId = ((Integer) op[1]).intValue();
            Map<String, String> meta = store.getMetaMap();
            String m = meta.get("map." + mapId);
            String mapName = DataUtils.parseMap(m).get("name");
            MVMap<Object, Object[]> map = store.openMap(mapName);
            Object key = op[2];
            Object[] value = map.get(key);
            if (value != null) {
                Long oldVersion = (Long) value[1];
                if (oldVersion == null) {
                    // this transaction added the value
                    map.remove(key);
                } else if (oldVersion < map.getCreateVersion()) {
                    map.remove(key);
                } else {
                    // this transaction updated the value
                    MVMap<Object, Object[]> mapOld = map
                            .openVersion(oldVersion);
                    Object[] old = mapOld.get(key);
                    if (old == null) {
                        map.remove(key);
                    } else {
                        map.put(key, old);
                    }
                }
            }
            undoLog.remove(logId);
        }
        store.commit();
    }

    /**
     * A transaction.
     */
    public static class Transaction {

        /**
         * The status of an open transaction.
         */
        public static final int STATUS_OPEN = 0;

        /**
         * The status of a prepared transaction.
         */
        public static final int STATUS_PREPARED = 1;

        /**
         * The status of a closed transaction (committed or rolled back).
         */
        public static final int STATUS_CLOSED = 2;

        /**
         * The operation type for changes in a map.
         */
        static final int OP_REMOVE = 0, OP_ADD = 1, OP_SET = 2;

        /**
         * The transaction store.
         */
        final TransactionStore store;
        
        /**
         * The version of the store at the time the transaction was started.
         */
        final long startVersion;

        /**
         * The transaction id.
         */
        final long transactionId;

        private int status;

        private String name;

        private long logId;

        Transaction(TransactionStore store, long transactionId, int status, String name, long logId) {
            this.store = store;
            this.startVersion = store.store.getCurrentVersion();
            this.transactionId = transactionId;
            this.status = status;
            this.name = name;
            this.logId = logId;
        }

        /**
         * Get the transaction id.
         *
         * @return the transaction id
         */
        public long getId() {
            return transactionId;
        }

        /**
         * Get the transaction status.
         *
         * @return the status
         */
        public int getStatus() {
            return status;
        }

        /**
         * Set the name of the transaction.
         *
         * @param name the new name
         */
        public void setName(String name) {
            checkOpen();
            store.setTransactionName(this, name);
            this.name = name;
        }

        /**
         * Get the name of the transaction.
         *
         * @return name the name
         */
        public String getName() {
            return name;
        }

        /**
         * Create a new savepoint.
         *
         * @return the savepoint id
         */
        public long setSavepoint() {
            checkOpen();
            store.store.incrementVersion();
            return logId;
        }

        /**
         * Add a log entry.
         *
         * @param opType the operation type
         * @param mapId the map id
         * @param key the key
         */
        void log(int opType, int mapId, Object key) {
            store.log(this, logId++, opType, mapId, key);
        }

        /**
         * Open a data map where reads are always up to date.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name) {
            return openMap(name, -1);
        }

        /**
         * Open a data map where reads are based on the specified version / savepoint.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @param readVersion the version used for reading
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name, long readVersion) {
            checkOpen();
            return new TransactionMap<K, V>(this, name, new ObjectDataType(), 
                    new ObjectDataType(), readVersion);
        }

        /**
         * Open the map to store the data.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @param readVersion the version used for reading
         * @param builder the builder
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name, long readVersion, Builder<K, V> builder) {
            checkOpen();
            DataType keyType = builder.getKeyType();
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            DataType valueType = builder.getValueType();
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new TransactionMap<K, V>(this, name, keyType, valueType, readVersion);
        }

        /**
         * Roll back to the given savepoint. This is only allowed if the
         * transaction is open.
         *
         * @param savepointId the savepoint id
         */
        public void rollbackToSavepoint(long savepointId) {
            checkOpen();
            store.rollbackTo(this, this.logId, savepointId);
            this.logId = savepointId;
        }

        /**
         * Prepare the transaction. Afterwards, the transaction can only be
         * committed or rolled back.
         */
        public void prepare() {
            checkOpen();
            store.prepare(this);
            status = STATUS_PREPARED;
        }

        /**
         * Commit the transaction. Afterwards, this transaction is closed.
         */
        public void commit() {
            if (status != STATUS_CLOSED) {
                store.commit(this, logId);
                status = STATUS_CLOSED;
            }
        }

        /**
         * Roll the transaction back. Afterwards, this transaction is closed.
         */
        public void rollback() {
            if (status != STATUS_CLOSED) {
                store.rollback(this, logId);
                status = STATUS_CLOSED;
            }
        }

        /**
         * Check whether this transaction is still open.
         */
        void checkOpen() {
            if (status != STATUS_OPEN) {
                throw DataUtils.newIllegalStateException("Transaction is closed");
            }
        }

        public long getCurrentVersion() {
            return store.store.getCurrentVersion();
        }

    }

    /**
     * A map that supports transactions.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class TransactionMap<K, V> {

        private Transaction transaction;

        private final int mapId;

        /**
         * The map used for writing (the latest version).
         * <p>
         * Key: key the key of the data.
         * Value: { transactionId, oldVersion, value }
         */
        private final MVMap<K, Object[]> mapWrite;

        /**
         * The map used for reading (possibly an older version). Reading is done
         * on an older version so that changes are not immediately visible, to
         * support statement processing (for example
         * "update test set id = id + 1").
         * <p>
         * Key: key the key of the data.
         * Value: { transactionId, oldVersion, value }
         */
        private final MVMap<K, Object[]> mapRead;

        TransactionMap(Transaction transaction, String name, DataType keyType, 
                DataType valueType, long readVersion) {
            this.transaction = transaction;
            ArrayType arrayType = new ArrayType(new DataType[] {
                    new ObjectDataType(), new ObjectDataType(), valueType
            });
            MVMap.Builder<K, Object[]> builder = new MVMap.Builder<K, Object[]>()
                    .keyType(keyType).valueType(arrayType);
            mapWrite = transaction.store.store.openMap(name, builder);
            mapId = mapWrite.getId();
            if (readVersion >= 0) {
                mapRead = mapWrite.openVersion(readVersion);
            } else {
                mapRead = mapWrite;
            }
        }

        /**
         * Get the size of the map as seen by this transaction.
         *
         * @return the size
         */
        public long getSize() {
            // TODO this method is very slow
            long size = 0;
            Cursor<K> cursor = mapRead.keyIterator(null);
            while (cursor.hasNext()) {
                K key = cursor.next();
                if (get(key) != null) {
                    size++;
                }
            }
            return size;
        }

        private void checkOpen() {
            transaction.checkOpen();
        }

        /**
         * Remove an entry.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V remove(K key) {
            return set(key, null);
        }

        /**
         * Update the value for the given key.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @param value the new value (not null)
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V put(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return set(key, value);
        }

        private V set(K key, V value) {
            checkOpen();
            long start = 0;
            while (true) {
                V old = get(key);
                boolean ok = trySet(key, value, false);
                if (ok) {
                    return old;
                }
                // an uncommitted transaction:
                // wait until it is committed, or until the lock timeout
                long timeout = transaction.store.lockTimeout;
                if (timeout == 0) {
                    throw DataUtils.newIllegalStateException("Lock timeout");
                }
                if (start == 0) {
                    start = System.currentTimeMillis();
                } else {
                    long t = System.currentTimeMillis() - start;
                    if (t > timeout) {
                        throw DataUtils.newIllegalStateException("Lock timeout");
                    }
                    // TODO use wait/notify instead
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
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
            return trySet(key, null, false);
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
            return trySet(key, value, false);
        }

        /**
         * Try to set or remove the value. When updating only unchanged entries,
         * then the value is only changed if it was not changed after opening
         * the map.
         *
         * @param key the key
         * @param value the new value (null to remove the value)
         * @param onlyIfUnchanged only set the value if it was not changed (by
         *            this or another transaction) since the map was opened
         * @return true if the value was set
         */
        public boolean trySet(K key, V value, boolean onlyIfUnchanged) {
            MVMap<K, Object[]> m = mapRead;
            Object[] current = mapWrite.get(key);
            if (onlyIfUnchanged) {
                Object[] old = m.get(key);
                if (!mapWrite.areValuesEqual(old, current)) {
                    long tx = (Long) current[0];
                    if (tx == transaction.transactionId) {
                        if (value == null) {
                            // ignore removing an entry
                            // if it was added or changed
                            // in the same statement
                            return true;
                        } else if (current[2] == null) {
                            // add an entry that was removed
                            // in the same statement
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
            long oldVersion = transaction.store.store.getCurrentVersion() - 1;
            int opType;
            if (current == null || current[2] == null) {
                if (value == null) {
                    // remove a removed value
                    opType = Transaction.OP_SET;
                } else {
                    opType = Transaction.OP_ADD;
                }
            } else {
                if (value == null) {
                    opType = Transaction.OP_REMOVE;
                } else {
                    opType = Transaction.OP_SET;
                }
            }
            Object[] newValue = { transaction.transactionId, oldVersion, value };
            if (current == null) {
                // a new value
                newValue[1] = null;
                Object[] old = mapWrite.putIfAbsent(key, newValue);
                if (old == null) {
                    transaction.log(opType, mapId, key);
                    return true;
                }
                return false;
            }
            long tx = (Long) current[0];
            if (tx == transaction.transactionId) {
                // added or updated by this transaction
                if (mapWrite.replace(key, current, newValue)) {
                    if (current[1] == null) {
                        transaction.log(opType, mapId, key);
                    } else {
                        long c = (Long) current[1];
                        if (c != oldVersion) {
                            transaction.log(opType, mapId, key);
                        }
                    }
                    return true;
                }
                // strange, somebody overwrite the value
                // even thought the change was not committed
                return false;
            }
            // added or updated by another transaction
            boolean open = transaction.store.openTransactions.containsKey(tx);
            if (!open) {
                // the transaction is committed:
                // overwrite the value
                if (mapWrite.replace(key, current, newValue)) {
                    transaction.log(opType, mapId, key);
                    return true;
                }
                // somebody else was faster
                return false;
            }
            // the transaction is not yet committed
            return false;
        }

        /**
         * Get the value for the given key at the time when this map was opened.
         *
         * @param key the key
         * @return the value or null
         */
        public V get(K key) {
            return get(key, mapRead);
        }

        /**
         * Get the most recent value for the given key.
         *
         * @param key the key
         * @return the value or null
         */
        public V getLatest(K key) {
            return get(key, mapWrite);
        }
        
        public boolean containsKey(K key) {
            return get(key) != null;
        }

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @param m the map
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public V get(K key, MVMap<K, Object[]> m) {
            checkOpen();
            while (true) {
                Object[] data = m.get(key);
                long tx;
                if (data == null) {
                    // doesn't exist or deleted by a committed transaction
                    return null;
                }
                tx = (Long) data[0];
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    return (V) data[2];
                }
                // added or updated by another transaction
                boolean open = transaction.store.openTransactions.containsKey(tx);
                if (!open) {
                    // it is committed
                    return (V) data[2];
                }
                tx = (Long) data[0];
                // get the value before the uncommitted transaction
                if (data[1] == null) {
                    // a new entry
                    return null;
                }
                long oldVersion = (Long) data[1];
                m = mapWrite.openVersion(oldVersion);
            }
        }

        public void renameMap(String newMapName) {
            // TODO rename maps transactionally
            mapWrite.renameMap(newMapName);
        }

        public boolean isClosed() {
            return mapWrite.isClosed();
        }

        public void removeMap() {
            // TODO remove in a transaction
            mapWrite.removeMap();
        }

        public void clear() {
            // TODO truncate transactionally
            mapWrite.clear();
        }

        public K firstKey() {
            // TODO transactional firstKey
            return mapRead.firstKey();
        }

        public K lastKey() {
            // TODO transactional lastKey
            return mapRead.lastKey();
        }

        public Iterator<K> keyIterator(K from) {
            // TODO transactional keyIterator
            return mapRead.keyIterator(from);
        }

        public K ceilingKey(K key) {
            // TODO transactional ceilingKey
            return mapRead.ceilingKey(key);
        }

        public K higherKey(K key) {
            // TODO transactional higherKey
            return mapRead.higherKey(key);
        }

        public K lowerKey(K key) {
            // TODO Auto-generated method stub
            return mapRead.lowerKey(key);
        }

        public Transaction getTransaction() {
            return transaction;
        }

    }
    
    /**
     * A data type that contains an array of objects with the specified data
     * types.
     */
    public static class ArrayType implements DataType {

        private final int arrayLength;
        private final DataType[] elementTypes;

        ArrayType(DataType[] elementTypes) {
            this.arrayLength = elementTypes.length;
            this.elementTypes = elementTypes;
        }

        @Override
        public int getMemory(Object obj) {
            Object[] array = (Object[]) obj;
            int size = 0;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                size += t.getMemory(array[i]);
            }
            return size;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            Object[] a = (Object[]) aObj;
            Object[] b = (Object[]) bObj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                int comp = t.compare(a[i], b[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        }

        @Override
        public ByteBuffer write(ByteBuffer buff, Object obj) {
            Object[] array = (Object[]) obj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                t.write(buff, array[i]);
            }
            return buff;
        }

        @Override
        public Object read(ByteBuffer buff) {
            Object[] array = new Object[arrayLength];
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                array[i] = t.read(buff);
            }
            return array;
        }   
        
    }

}

