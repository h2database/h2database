/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Iterator;
import java.util.Map;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMapConcurrent;
import org.h2.mvstore.MVStore;
import org.h2.test.TestBase;

/**
 * Test concurrent transactions.
 */
public class TestTransactionMap extends TestBase {

    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() {
        MVStore s = MVStore.open(null);

        TransactionalStore ts = new TransactionalStore(s);
        
        Transaction tx = ts.begin();
        TransactionalMap<String, String> m = tx.openMap("test");
        m.put("1", "Hello");
        assertEquals("Hello", m.get("1"));
        m.put("2", "World");
        assertEquals("World", m.get("2"));
        tx.rollback();
        assertNull(m.get("1"));
        assertNull(m.get("2"));

        s.close();
    }

    /**
     * A store that supports concurrent transactions.
     */
    static class TransactionalStore {

        final MVStore store;

        // key: transactionId, value: baseVersion
        final MVMap<Long, Long> openTransactions;

        // key: [ transactionId, logId ], value: [ baseVersion, mapId, key ]
        final MVMap<long[], Object[]> undoLog;

        long lockTimeout = 1000;

        TransactionalStore(MVStore store) {
            this.store = store;
            openTransactions = store.openMap("openTransactions",
                    new MVMapConcurrent.Builder<Long, Long>());
            undoLog = store.openMap("undoLog",
                    new MVMapConcurrent.Builder<long[], Object[]>());
        }

        void init() {
            // TODO rollback all old, stored transactions (if there are any)
        }

        Transaction begin() {
            long baseVersion;
            long transactionId;
            // repeat if necessary (when transactions are created concurrently)
            // might use synchronization instead
            while (true) {
                baseVersion = store.incrementVersion();
                Long t = openTransactions.lastKey();
                transactionId = t == null ? 0 : t.longValue() + 1;
                t = openTransactions.putIfAbsent(transactionId, baseVersion);
                if (t == null) {
                    break;
                }
            }
            return new Transaction(this, transactionId);
        }

        // TODO rollback in reverse order, 
        // to support delete & add of the same key
        // with different baseVersions
        // TODO return the undo operations instead
        public void endTransaction(boolean success, long transactionId) {
            Iterator<long[]> it = undoLog.keyIterator(new long[] {
                    transactionId, 0 });
            store.incrementVersion();
            while (it.hasNext()) {
                long[] k = it.next();
                if (k[0] != transactionId) {
                    break;
                }
                Object[] op = undoLog.get(k);
                int mapId = ((Integer) op[1]).intValue();
                Map<String, String> meta = store.getMetaMap();
                String m = meta.get("map." + mapId);
                String mapName = DataUtils.parseMap(m).get("name");
                MVMap<Object, Object[]> map = store.openMap(mapName);
                Object key = op[2];
                if (success) {
                    Object[] value = map.get(key);
                    if (value[1] == null) {
                        // remove the value
                        map.remove(key);
                    }
                } else {
                    long baseVersion = ((Long) op[0]).longValue();
                    Object[] v = map.get(key);
                    Object value = v[1];
                    Object[] old;
                    if (baseVersion >= map.getCreateVersion()) {
                        // the map didn't exist yet
                        old = null;
                    } else {
                        MVMap<Object, Object[]> mapOld = map
                                .openVersion(baseVersion - 1);
                        old = mapOld.get(key);
                    }

                    
                    if (value == null) {
                        // this transaction deleted the value
                        map.put(key, old);
                    } else {
                        if (old == null) {
                            // this transaction added the value
                            map.remove(key);
                        } else {
                            // this transaction updated the value
                            map.put(key, old);
                        }
                    }
                }
                undoLog.remove(k);
            }
            openTransactions.remove(transactionId);
            store.commit();
        }

    }

    /**
     * A transaction.
     */
    static class Transaction {
        final TransactionalStore store;
        final long transactionId;
        long logId;
        long baseVersion;

        Transaction(TransactionalStore store, long transactionId) {
            this.store = store;
            this.transactionId = transactionId;
            this.baseVersion = store.store.incrementVersion();
        }

        void log(long baseVersion, int mapId, Object key) {
            long[] undoKey = { transactionId, logId++ };
            Object[] log = new Object[] { baseVersion, mapId, key };
            store.undoLog.put(undoKey, log);
        }

        <K, V> TransactionalMap<K, V> openMap(String name) {
            return new TransactionalMap<K, V>(this, name);
        }

        void commit() {
            store.endTransaction(true, transactionId);
        }

        void rollback() {
            store.endTransaction(false, transactionId);
        }

    }

    /**
     * A map that supports transactions.
     * 
     * @param <K> the key type
     * @param <V> the value type
     */
    static class TransactionalMap<K, V> {

        private Transaction transaction;

        /**
         * The newest version of the data. Key: key Value: { transactionId,
         * value }
         */
        private final MVMap<K, Object[]> map;
        private final int mapId;
        private final MVMap<K, Object[]> oldMap;

        TransactionalMap(Transaction transaction, String name) {
            this.transaction = transaction;
            map = transaction.store.store.openMap(name);
            mapId = map.getId();
            oldMap = map.openVersion(transaction.baseVersion);
        }

        void put(K key, V value) {
            long start = 0;
            while (true) {
                Object[] current = map.get(key);
                Object[] newValue = { transaction.transactionId, value };
                if (current == null) {
                    // a new value
                    Object[] old = map.putIfAbsent(key, newValue);
                    if (old == null) {
                        transaction.log(transaction.baseVersion, mapId, key);
                        return;
                    }
                    // retry
                    continue;
                }
                Object[] old = oldMap.get(key);
                long tx = ((Long) old[0]).longValue();
                if (tx == transaction.transactionId) {
                    // update using the same transaction
                    if (map.replace(key, current, newValue)) {
                        transaction.log(transaction.baseVersion, mapId, key);
                        return;
                    }
                    // retry
                    continue;
                }
                Long base = transaction.store.openTransactions.get(tx);
                if (base == null) {
                    // a committed transaction
                    // overwrite the value
                    if (map.replace(key, old, newValue)) {
                        transaction.log(transaction.baseVersion, mapId, key);
                        return;
                    }
                    // retry
                    continue;
                }
                // an uncommitted transaction:
                // wait until it is committed, or until the lock timeout
                if (start == 0) {
                    start = System.currentTimeMillis();
                } else {
                    long t = System.currentTimeMillis() - start;
                    if (t > transaction.store.lockTimeout) {
                        throw new IllegalStateException("Lock timeout");
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        V get(K key) {
            Object[] old = oldMap.get(key);
            Object[] current = map.get(key);
            if (old == null) {
                if (current == null) {
                    // didn't exist before and doesn't exist now
                    return null;
                }
                long tx = ((Long) current[0]).longValue();
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    return (V) current[1];
                }
                // added by another transaction
                return null;
            } else if (current == null) {
                // deleted by a committed transaction
                // which means not by the current transaction
                return (V) old[1];
            }
            long tx = ((Long) current[0]).longValue();
            if (tx == transaction.transactionId) {
                // updated by this transaction
                return (V) current[1];
            }
            // updated by another transaction
            Long base = transaction.store.openTransactions.get(transaction);
            if (base == null) {
                // it was committed
                return (V) old[1];
            }
            // get the value before the uncommitted transaction
            MVMap<K, Object[]> olderMap = oldMap.openVersion(base.longValue());
            old = olderMap.get(key);
            if (old == null) {
                // there was none
                return null;
            }
            // the previous committed value
            return (V) old[1];
        }

    }
}
