/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Map;


/**
 * A store that supports concurrent transactions.
 */
public class TransactionStore {

    /**
     * The store.
     */
    final MVStore store;

    /**
     * The map of open transaction.
     * Key: transactionId, value: baseVersion.
     */
    final MVMap<Long, Long> openTransactions;

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

    private long lastTransactionId;

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this.store = store;
        settings = store.openMap("settings");
        openTransactions = store.openMap("openTransactions",
                new MVMapConcurrent.Builder<Long, Long>());
        // TODO one undo log per transaction to speed up commit
        // (alternative: add a range delete operation for maps)
        undoLog = store.openMap("undoLog",
                new MVMapConcurrent.Builder<long[], Object[]>());
        init();
    }

    private void init() {
        String s = settings.get("lastTransaction");
        if (s != null) {
            lastTransactionId = Long.parseLong(s);
        }
        Long t = openTransactions.lastKey();
        if (t != null) {
            if (t.longValue() > lastTransactionId) {
                throw DataUtils.newIllegalStateException("Last transaction not stored");
            }
            // TODO rollback all old, stored transactions (if there are any)
        }
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        settings.put("lastTransaction", "" + lastTransactionId);
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public synchronized Transaction begin() {
        long baseVersion = store.getCurrentVersion();
        store.incrementVersion();
        long transactionId = lastTransactionId++;
        if (lastTransactionId % 32 == 0) {
            settings.put("lastTransaction", "" + lastTransactionId + 32);
        }
        openTransactions.put(transactionId, baseVersion);
        return new Transaction(this, transactionId);
    }

    /**
     * Commit a transaction.
     *
     * @param transactionId the transaction id
     * @param maxLogId the last log id
     */
    void commit(long transactionId, long maxLogId) {
        // TODO commit should be much faster
        store.incrementVersion();
        for (long logId = 0; logId < maxLogId; logId++) {
            Object[] op = undoLog.get(new long[] {
                    transactionId, logId });
            int mapId = ((Integer) op[1]).intValue();
            Map<String, String> meta = store.getMetaMap();
            String m = meta.get("map." + mapId);
            String mapName = DataUtils.parseMap(m).get("name");
            MVMap<Object, Object[]> map = store.openMap(mapName);
            Object key = op[2];
            Object[] value = map.get(key);
            if (value == null) {
                // already removed
            } else if (value[2] == null) {
                // remove the value
                map.remove(key);
            }
            undoLog.remove(logId);
        }
        openTransactions.remove(transactionId);
        store.commit();
    }

    /**
     * Roll a transaction back.
     *
     * @param transactionId the transaction id
     * @param maxLogId the last log id
     */
    void rollback(long transactionId, long maxLogId) {
        rollbackTo(transactionId, maxLogId, 0);
        openTransactions.remove(transactionId);
        store.commit();
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param transactionId the transaction id
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(long transactionId, long maxLogId, long toLogId) {
        store.incrementVersion();
        for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
            Object[] op = undoLog.get(new long[] {
                    transactionId, logId });
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
         * The transaction store.
         */
        final TransactionStore store;

        /**
         * The transaction id.
         */
        final long transactionId;

        private long logId;
        private boolean closed;

        Transaction(TransactionStore store, long transactionId) {
            this.store = store;
            this.transactionId = transactionId;
        }

        /**
         * Create a new savepoint.
         *
         * @return the savepoint id
         */
        public long setSavepoint() {
            store.store.incrementVersion();
            return logId;
        }

        /**
         * Add a log entry.
         *
         * @param baseVersion the old version
         * @param mapId the map id
         * @param key the key
         */
        void log(long baseVersion, int mapId, Object key) {
            long[] undoKey = { transactionId, logId++ };
            Object[] log = new Object[] { baseVersion, mapId, key };
            store.undoLog.put(undoKey, log);
        }

        /**
         * Open a data map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name) {
            return new TransactionMap<K, V>(this, name);
        }

        /**
         * Commit the transaction. Afterwards, this transaction is closed.
         */
        public void commit() {
            closed = true;
            store.commit(transactionId, logId);
        }

        /**
         * Roll the transaction back. Afterwards, this transaction is closed.
         */
        public void rollback() {
            closed = true;
            store.rollback(transactionId, logId);
        }

        /**
         * Roll back to the given savepoint.
         *
         * @param savepointId the savepoint id
         */
        public void rollbackToSavepoint(long savepointId) {
            store.rollbackTo(transactionId, this.logId, savepointId);
            this.logId = savepointId;
        }

        /**
         * Check whether this transaction is still open.
         */
        void checkOpen() {
            if (closed) {
                throw DataUtils.newIllegalStateException("Transaction is closed");
            }
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

        /**
         * The newest version of the data.
         * Key: key.
         * Value: { transactionId, oldVersion, value }
         */
        private final MVMap<K, Object[]> map;
        private final int mapId;

        TransactionMap(Transaction transaction, String name) {
            this.transaction = transaction;
            map = transaction.store.store.openMap(name);
            mapId = map.getId();
        }

        /**
         * Get the size of the map as seen by this transaction.
         *
         * @return the size
         */
        public long size() {
            // TODO this method is very slow
            long size = 0;
            Cursor<K> cursor = map.keyIterator(null);
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
         * Update the value for the given key. If the row is locked, this method
         * will retry until the row could be updated or until a lock timeout.
         *
         * @param key the key
         * @param value the new value (null to remove the row)
         * @throws IllegalStateException if a lock timeout occurs
         */
        public void put(K key, V value) {
            checkOpen();
            long start = 0;
            while (true) {
                boolean ok = tryPut(key, value);
                if (ok) {
                    return;
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
         * Try to update the value for the given key. This will fail if the row
         * is not locked by another transaction (that means, if another open
         * transaction added or updated the row).
         *
         * @param key the key
         * @param value the new value
         * @return whether the value could be updated
         */
        public boolean tryPut(K key, V value) {
            Object[] current = map.get(key);
            long oldVersion = transaction.store.store.getCurrentVersion() - 1;
            Object[] newValue = { transaction.transactionId, oldVersion, value };
            if (current == null) {
                // a new value
                newValue[1] = null;
                Object[] old = map.putIfAbsent(key, newValue);
                if (old == null) {
                    transaction.log(oldVersion, mapId, key);
                    return true;
                }
                return false;
            }
            long tx = ((Long) current[0]).longValue();
            if (tx == transaction.transactionId) {
                // added or updated by this transaction
                if (map.replace(key, current, newValue)) {
                    if (current[1] == null) {
                        transaction.log(oldVersion, mapId, key);
                    } else {
                        long c = (Long) current[1];
                        if (c != oldVersion) {
                            transaction.log(oldVersion, mapId, key);
                        }
                    }
                    return true;
                }
                // strange, somebody overwrite the value
                // even thought the change was not committed
                return false;
            }
            // added or updated by another transaction
            Long base = transaction.store.openTransactions.get(tx);
            if (base == null) {
                // the transaction is committed:
                // overwrite the value
                if (map.replace(key, current, newValue)) {
                    transaction.log(oldVersion, mapId, key);
                    return true;
                }
                // somebody else was faster
                return false;
            }
            // the transaction is not yet committed
            return false;
        }

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public
        V get(K key) {
            checkOpen();
            MVMap<K, Object[]> m = map;
            while (true) {
                Object[] data = m.get(key);
                long tx;
                if (data == null) {
                    // doesn't exist or deleted by a committed transaction
                    return null;
                }
                tx = ((Long) data[0]).longValue();
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    return (V) data[2];
                }
                // added or updated by another transaction
                Long base = transaction.store.openTransactions.get(tx);
                if (base == null) {
                    // it is committed
                    return (V) data[2];
                }
                tx = ((Long) data[0]).longValue();
                // get the value before the uncommitted transaction
                if (data[1] == null) {
                    // a new entry
                    return null;
                }
                long oldVersion = (Long) data[1];
                m = map.openVersion(oldVersion);
            }
        }

    }

}

