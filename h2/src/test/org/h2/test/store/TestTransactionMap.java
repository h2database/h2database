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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.h2.mvstore.Cursor;
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
        testConcurrentTransactionsReadCommitted();
        testSingleConnection();
        testCompareWithPostgreSQL();
    }
    
    public void testCompareWithPostgreSQL() throws Exception {
        ArrayList<Statement> statements = New.arrayList();
        ArrayList<Transaction> transactions = New.arrayList();
        ArrayList<TransactionalMap<Integer, String>> maps = New.arrayList();
        int connectionCount = 3, opCount = 1000, rowCount = 10;
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
            // 100 ms to avoid blocking (the test is single threaded)
            stat.execute("set statement_timeout to 100");
            Connection c = stat.getConnection();
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
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
                    buff.append(i).append(": [" + connIndex + "]=");
                    int size = 0;
                    while (rs.next()) {
                        buff.append(' ');
                        int k = rs.getInt(1);
                        String v = rs.getString(2);
                        buff.append(k).append(':').append(v);
                        assertEquals(v, map.get(k));
                        size++;
                    }
                    buff.append('\n');
                    if (size != map.size()) {
                        assertEquals(size, map.size());
                    }
                }
                int x = r.nextInt(rowCount);
                int y = r.nextInt(rowCount);
                buff.append(i).append(": [" + connIndex + "]: ");
                ResultSet rs = null;
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
                    String old = map.get(x);
                    if (old == null) {
                        buff.append("insert " + x + "=" + y);
                        if (map.tryPut(x, "" + y)) {
                            stat.execute("insert into test values(" + x + ", '" + y + "')");
                        } else {
                            buff.append(" -> row was locked");
                            // the statement would time out in PostgreSQL
                            // TODO test sometimes if timeout occurs
                        }
                    } else {
                        buff.append("update " + x + "=" + y + " (old:" + old + ")");
                        if (map.tryPut(x, "" + y)) {
                            int c = stat.executeUpdate("update test set name = '" + y
                                    + "' where id = " + x);
                            assertEquals(1, c);
                        } else {
                            buff.append(" -> row was locked");
                            // the statement would time out in PostgreSQL
                            // TODO test sometimes if timeout occurs
                        }
                    }
                    break;
                case 3:
                    buff.append("delete " + x);
                    try {
                        int c = stat.executeUpdate("delete from test where id = " + x);
                        if (c == 1) {
                            map.put(x, null);
                        } else {
                            assertNull(map.get(x));
                        }
                    } catch (SQLException e) {
                        assertTrue(map.get(x) != null);
                        assertFalse(map.tryPut(x, null));
                        // PostgreSQL needs to rollback
                        buff.append(" -> rollback");
                        stat.getConnection().rollback();
                        transaction.rollback();
                        transactions.set(connIndex, null);
                    }
                    break;
                case 4:
                case 5:
                case 6:
                    rs = stat.executeQuery("select * from test where id = " + x);
                    String expected = rs.next() ? rs.getString(2) : null;
                    buff.append("select " + x + "=" + expected);
                    assertEquals("i:" + i, expected, map.get(x));
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
    
    public void testConcurrentTransactionsReadCommitted() {
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
        
        assertEquals("Hello", m2.get("1"));
        assertEquals("World", m2.get("2"));
        assertNull(m2.get("3"));

        tx1.commit();

        assertEquals("Hallo", m2.get("1"));
        assertNull(m2.get("2"));
        assertEquals("!", m2.get("3"));
        
        tx1 = ts.begin();
        m1 = tx1.openMap("test");
        m1.put("2", "World");
        
        assertNull(m2.get("2"));
        assertFalse(m2.tryPut("2", null));
        assertFalse(m2.tryPut("2", "Welt"));

        tx2 = ts.begin();
        m2 = tx2.openMap("test");
        m1.put("2", null);
        assertNull(m2.get("2"));

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
            // TODO one undo log per transaction to speed up commit
            // (alternative: add a range delete operation for maps)
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
                Object[] value = map.get(key);
                if (success) {
                    if (value[2] == null) {
                        // remove the value
                        map.remove(key);
                    }
                } else if (value == null) {
                    // the value was deleted afterwards
                    // TODO how can this happen?
                } else {
                    Long oldVersion = (Long) value[1];
                    if (oldVersion == null) {
                        // this transaction added the value
                        map.remove(key);
                    } else {
                        // this transaction updated the value
                        MVMap<Object, Object[]> mapOld = map
                                .openVersion(oldVersion);
                        Object[] old = mapOld.get(key);
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
        private boolean closed;

        Transaction(TransactionalStore store, long transactionId) {
            this.store = store;
            this.transactionId = transactionId;
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
         * The newest version of the data. 
         * Key: key. 
         * Value: { transactionId, oldVersion, value }
         */
        private final MVMap<K, Object[]> map;
        private final int mapId;

        TransactionalMap(Transaction transaction, String name) {
            this.transaction = transaction;
            map = transaction.store.store.openMap(name);
            mapId = map.getId();
        }
        
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
                // use the previous oldVersion
                newValue[1] = current[1];
                if (map.replace(key, current, newValue)) {
                    // we already have a log entry, so don't log
                    // TODO does not work when using savepoints
                    // transaction.log(oldVersion, mapId, key);
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

        @SuppressWarnings("unchecked")
        V get(K key) {
            checkOpen();
            Object[] current = map.get(key);
            long tx;
            if (current == null) {
                // doesn't exist or deleted by a committed transaction
                return null;
            }
            tx = ((Long) current[0]).longValue();
            if (tx == transaction.transactionId) {
                // added by this transaction
                return (V) current[2];
            }
            // added or updated by another transaction
            Long base = transaction.store.openTransactions.get(tx);
            if (base == null) {
                // it is committed
                return (V) current[2];
            }                
            tx = ((Long) current[0]).longValue();
            // get the value before the uncommitted transaction
            if (current[1] == null) {
                // a new entry
                return null;
            }
            long oldVersion = (Long) current[1];
            MVMap<K, Object[]> oldMap = map.openVersion(oldVersion);
            Object[] old = oldMap.get(key);
            if (old == null) {
                // there was none
                return null;
            }
            // the previous committed value
            return (V) old[2];
        }

    }
    
}
