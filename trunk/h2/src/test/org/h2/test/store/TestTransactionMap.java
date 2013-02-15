/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMapConcurrent;
import org.h2.mvstore.MVStore;
import org.h2.test.TestBase;
import org.h2.util.New;

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

    public void test() throws Exception {
        testConcurrentTransactions();
        testSingleConnection();
        // testCompareWithPostgreSQL();
    }
    
    public void testCompareWithPostgreSQL() throws Exception {
        ArrayList<Statement> statements = New.arrayList();
        ArrayList<Transaction> transactions = New.arrayList();
        ArrayList<TransactionalMap<Integer, String>> maps = New.arrayList();
        int connectionCount = 4, opCount = 1000, rowCount = 10;
        try {
            Class.forName("org.postgresql.Driver");
            for (int i = 0; i < connectionCount; i++) {
                Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql:test", "sa", "sa");
                statements.add(conn.createStatement());
            }
        } catch (Exception e) {
            // database not installed - ok
            return;
        }
        statements.get(0).execute(
                "drop table if exists test");
        statements.get(0).execute(
                "create table test(id int primary key, name varchar(255))");
        
        MVStore s = MVStore.open(null);
        TransactionalStore ts = new TransactionalStore(s);
        for (int i = 0; i < connectionCount; i++) {
            Statement stat = statements.get(i);
            Connection c = stat.getConnection();
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            Transaction transaction = ts.begin();
            transactions.add(transaction);
            TransactionalMap<Integer, String> map;
            map = transaction.openMap("test");
            maps.add(map);
        }
        StringBuilder buff = new StringBuilder();
        
        Random r = new Random(1);
        try {
            for (int i = 0; i < opCount; i++) {
                int connIndex = r.nextInt(connectionCount);
                Statement stat = statements.get(connIndex);
                Transaction transaction = transactions.get(connIndex);
                TransactionalMap<Integer, String> map = maps.get(connIndex);
                if (transaction == null) {
                    transaction = ts.begin();
                    map = transaction.openMap("test");
                    transactions.set(connIndex, transaction);
                    maps.set(connIndex, map);
                    
                    // read all data, to get a snapshot
                    ResultSet rs = stat.executeQuery(
                            "select * from test order by id");
                    buff.append("[" + connIndex + "]=");
                    while (rs.next()) {
                        buff.append(' ');
                        buff.append(rs.getInt(1)).append(':').append(rs.getString(2));
                    }
                    buff.append('\n');
                }
                int x = r.nextInt(rowCount);
                int y = r.nextInt(rowCount);
                buff.append("[" + connIndex + "]: ");
                switch (r.nextInt(7)) {
                case 0:
                    buff.append("commit");
                    stat.getConnection().commit();
                    transaction.commit();
                    transactions.set(connIndex, null);
                    break;
                case 1:
                    buff.append("rollback");
                    stat.getConnection().rollback();
                    transaction.rollback();
                    transactions.set(connIndex, null);
                    break;
                case 2:
                    // insert or update
                    if (i == 98) {
                        int test;
                        System.out.println(map.get(x));
                    }
                    String old = map.get(x);
                    if (old == null) {
                        buff.append("insert " + x + "=" + y);
                        if (map.tryPut(x, "" + y)) {
                            stat.execute("insert into test values(" + x + ", '" + y + "')");
                        } else {
                            // TODO how to check for locked rows in PostgreSQL?
                        }
                    } else {
                        buff.append("update " + x + "=" + y + " (old:" + old + ")");
                        if (map.tryPut(x, "" + y)) {
                            int c = stat.executeUpdate("update test set name = '" + y
                                    + "' where id = " + x);
                            if (c == 0) {
                                int test;
                                System.out.println(map.get(x));
                            }
                            assertEquals(1, c);
                        } else {
                            // TODO how to check for locked rows in PostgreSQL?
                        }
                    }
                    break;
                case 3:
                    buff.append("delete " + x);
                    stat.execute("delete from test where id = " + x);
                    map.put(x, null);
                    break;
                case 4:
                case 5:
                case 6:
                    ResultSet rs = stat.executeQuery("select * from test where id = " + x);
                    String expected = rs.next() ? rs.getString(2) : null;
                    buff.append("select " + x + "=" + expected);
                    assertEquals(expected, map.get(x));
                    break;
                }
                buff.append('\n');
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(buff.toString());
        }
        for (Statement stat : statements) {
            stat.getConnection().close();
        }
    }
    
    public void testConcurrentTransactions() {
        MVStore s = MVStore.open(null);

        TransactionalStore ts = new TransactionalStore(s);
        
        Transaction tx1, tx2;
        TransactionalMap<String, String> m1, m2;

        tx1 = ts.begin();
        m1 = tx1.openMap("test");
        m1.put("1", "Hi");
        m1.put("3", ".");
        tx1.commit();

        tx1 = ts.begin();
        m1 = tx1.openMap("test");
        m1.put("1", "Hello");
        m1.put("2", "World");
        m1.put("3", null);
        tx1.commit();

        // start new transaction to read old data
        tx2 = ts.begin();
        m2 = tx2.openMap("test");
        
        // start transaction tx1, update/delete/add
        tx1 = ts.begin();
        m1 = tx1.openMap("test");
        m1.put("1", "Hallo");
        m1.put("2", null);
        m1.put("3", "!");
        tx1.commit();

        assertEquals("Hello", m2.get("1"));
        assertEquals("World", m2.get("2"));
        assertNull(m2.get("3"));
        
        // even thought the row is locked,
        // trying to remove it should work, as
        // this key is unknown to this map
        m2.put("3", null);
        // the row is locked, and trying to add a value
        // should fail
        assertFalse(m2.tryPut("3", "."));
        
        s.close();
    }
    
    public void testSingleConnection() {
        MVStore s = MVStore.open(null);

        TransactionalStore ts = new TransactionalStore(s);
        
        Transaction tx;
        TransactionalMap<String, String> m;
        
        // add, rollback
        tx = ts.begin();
        m = tx.openMap("test");
        m.put("1", "Hello");
        assertEquals("Hello", m.get("1"));
        m.put("2", "World");
        assertEquals("World", m.get("2"));
        tx.rollback();
        tx = ts.begin();
        m = tx.openMap("test");
        assertNull(m.get("1"));
        assertNull(m.get("2"));
        
        // add, commit
        tx = ts.begin();
        m = tx.openMap("test");
        m.put("1", "Hello");
        m.put("2", "World");
        assertEquals("Hello", m.get("1"));
        assertEquals("World", m.get("2"));
        tx.commit();
        tx = ts.begin();
        m = tx.openMap("test");
        assertEquals("Hello", m.get("1"));
        assertEquals("World", m.get("2"));
        
        // update+delete+insert, rollback
        tx = ts.begin();
        m = tx.openMap("test");
        m.put("1", "Hallo");
        m.put("2", null);
        m.put("3", "!");
        assertEquals("Hallo", m.get("1"));
        assertNull(m.get("2"));
        assertEquals("!", m.get("3"));
        tx.rollback();
        tx = ts.begin();
        m = tx.openMap("test");
        assertEquals("Hello", m.get("1"));
        assertEquals("World", m.get("2"));
        assertNull(m.get("3"));

        // update+delete+insert, commit
        tx = ts.begin();
        m = tx.openMap("test");
        m.put("1", "Hallo");
        m.put("2", null);
        m.put("3", "!");
        assertEquals("Hallo", m.get("1"));
        assertNull(m.get("2"));
        assertEquals("!", m.get("3"));
        tx.commit();
        tx = ts.begin();
        m = tx.openMap("test");
        assertEquals("Hallo", m.get("1"));
        assertNull(m.get("2"));
        assertEquals("!", m.get("3"));

        s.close();
    }

    /**
     * A store that supports concurrent transactions.
     */
    static class TransactionalStore {

        final MVStore store;

        /**
         * The transaction settings. "lastTransaction" the last transaction id.
         */
        final MVMap<String, String> settings;

        // key: transactionId, value: baseVersion
        final MVMap<Long, Long> openTransactions;

        // key: [ transactionId, logId ], value: [ baseVersion, mapId, key ]
        final MVMap<long[], Object[]> undoLog;

        long lastTransactionId;
        
        /**
         * The lock timeout in milliseconds. 0 means timeout immediately.
         */
        long lockTimeout;

        TransactionalStore(MVStore store) {
            this.store = store;
            settings = store.openMap("settings");
            openTransactions = store.openMap("openTransactions",
                    new MVMapConcurrent.Builder<Long, Long>());
            undoLog = store.openMap("undoLog",
                    new MVMapConcurrent.Builder<long[], Object[]>());
        }

        synchronized void init() {
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
        
        synchronized void close() {
            settings.put("lastTransaction", "" + lastTransactionId);
        }

        synchronized Transaction begin() {
            long baseVersion = store.getCurrentVersion();
            store.incrementVersion();
            long transactionId = lastTransactionId++;
            if (lastTransactionId % 32 == 0) {
                settings.put("lastTransaction", "" + lastTransactionId + 32);
            }
            openTransactions.put(transactionId, baseVersion);
            return new Transaction(this, transactionId);
        }

        // TODO rollback in reverse order, 
        // to support delete & add of the same key
        // with different baseVersions
        // TODO return the undo operations instead,
        // so that index changed can be undone
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
                    Object[] old;
                    if (baseVersion <= map.getCreateVersion()) {
                        // the map didn't exist yet
                        old = null;
                    } else {
                        MVMap<Object, Object[]> mapOld = map
                                .openVersion(baseVersion - 1);
                        old = mapOld.get(key);
                    }
                    if (old == null) {
                        // this transaction added the value
                        map.remove(key);
                    } else {
                        // this transaction updated the value
                        map.put(key, old);
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
        private boolean closed;

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
            closed = true;
            store.endTransaction(true, transactionId);
        }

        void rollback() {
            closed = true;
            store.endTransaction(false, transactionId);
        }

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
        
        private void checkOpen() {
            transaction.checkOpen();
        }

        void put(K key, V value) {
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
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        public boolean tryPut(K key, V value) {
            if (tryReplace(key, value)) {
                return true;
            }
            if (value == null) {
                // trying to remove a row that is invisible to this
                // transaction is a no-op
                return true;
            }
            return false;
        }
        
        public boolean tryReplace(K key, V value) {
            Object[] current = map.get(key);
            Object[] newValue = { transaction.transactionId, value };
            if (current == null) {
                // a new value
                Object[] old = map.putIfAbsent(key, newValue);
                if (old == null) {
                    transaction.log(transaction.baseVersion, mapId, key);
                    return true;
                }
                return false;
            }
            Object[] old = oldMap.get(key);
            if (old == null) {
                // added by another transaction
                return false;
            }
            long tx = ((Long) old[0]).longValue();
            if (tx == transaction.transactionId) {
                // update using the same transaction
                if (map.replace(key, current, newValue)) {
                    transaction.log(transaction.baseVersion, mapId, key);
                    return true;
                }
                return false;
            }
            Long base = transaction.store.openTransactions.get(tx);
            if (base == null) {
                // from a transaction that was committed 
                // when this transaction began:
                // overwrite the value
                if (map.replace(key, old, newValue)) {
                    transaction.log(transaction.baseVersion, mapId, key);
                    return true;
                }
            }
            return false;
        }

        @SuppressWarnings("unchecked")
        V get(K key) {
            checkOpen();
            Object[] old = oldMap.get(key);
            Object[] current = map.get(key);
            long tx;
            if (old == null) {
                if (current == null) {
                    // didn't exist before and doesn't exist now
                    return null;
                }
                tx = ((Long) current[0]).longValue();
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    return (V) current[1];
                }
                // added by another transaction
                return null;
            } else if (current == null) {
                // deleted by a committed transaction
                // which means not by the current transaction
                tx = ((Long) old[0]).longValue();
            } else {
                tx = ((Long) current[0]).longValue();
                if (tx == transaction.transactionId) {
                    // updated by this transaction
                    return (V) current[1];
                }
            }
            // updated by another transaction
            Long base = transaction.store.openTransactions.get(tx);
            if (base == null) {
                // it was committed
                return (V) old[1];
            }
            // get the value before the uncommitted transaction
            MVMap<K, Object[]> olderMap = map.openVersion(base.longValue());
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
