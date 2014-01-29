/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.store.InDoubtTransaction;
import org.h2.store.fs.FileChannelInputStream;
import org.h2.store.fs.FileUtils;
import org.h2.table.RegularTable;
import org.h2.table.TableBase;
import org.h2.util.New;

/**
 * A table engine that internally uses the MVStore.
 */
public class MVTableEngine implements TableEngine {

    /**
     * Initialize the MVStore.
     *
     * @param db the database
     * @return the store
     */
    public static Store init(final Database db) {
        Store store = db.getMvStore();
        if (store != null) {
            return store;
        }
        byte[] key = db.getFileEncryptionKey();
        String dbPath = db.getDatabasePath();
        MVStore.Builder builder = new MVStore.Builder();
        if (dbPath == null) {
            store = new Store(db, builder.open());
        } else {
            String fileName = dbPath + Constants.SUFFIX_MV_FILE;
            builder.fileName(fileName);
            if (db.isReadOnly()) {
                builder.readOnly();
            } else {
                // possibly create the directory
                boolean exists = FileUtils.exists(fileName);
                if (exists && !FileUtils.canWrite(fileName)) {
                    // read only
                } else {
                    String dir = FileUtils.getParent(fileName);
                    FileUtils.createDirectories(dir);
                }
            }
            if (key != null) {
                char[] password = new char[key.length / 2];
                for (int i = 0; i < password.length; i++) {
                    password[i] = (char) (((key[i + i] & 255) << 16) | ((key[i + i + 1]) & 255));
                }
                builder.encryptionKey(password);
            }
            builder.backgroundExceptionHandler(new UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    db.setBackgroundException(DbException.convert(e));
                }

            });
            try {
                store = new Store(db, builder.open());
            } catch (IllegalStateException e) {
                int errorCode = DataUtils.getErrorCode(e.getMessage());
                if (errorCode == DataUtils.ERROR_FILE_CORRUPT) {
                    if (key != null) {
                        throw DbException.get(ErrorCode.FILE_ENCRYPTION_ERROR_1, e, fileName);
                    }
                } else if (errorCode == DataUtils.ERROR_FILE_LOCKED) {
                    throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, e, fileName);
                }
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, e, fileName);
            }
        }
        db.setMvStore(store);
        return store;
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        if (!data.persistData) {
            return new RegularTable(data);
        }
        Store store = init(db);
        MVTable table = new MVTable(data, store);
        table.init(data.session);
        store.tableMap.put(table.getMapName(), table);
        return table;
    }

    /**
     * A store with open tables.
     */
    public static class Store {

        /**
         * The database.
         */
        final Database db;

        /**
         * The map of open tables.
         * Key: the map name, value: the table.
         */
        final ConcurrentHashMap<String, MVTable> tableMap = new ConcurrentHashMap<String, MVTable>();

        /**
         * The store.
         */
        private final MVStore store;

        /**
         * The transaction store.
         */
        private final TransactionStore transactionStore;

        private long statisticsStart;

        private int temporaryMapId;

        public Store(Database db, MVStore store) {
            this.db = db;
            this.store = store;
            this.transactionStore = new TransactionStore(store,
                    new ValueDataType(null, db, null));
        }

        public MVStore getStore() {
            return store;
        }

        public TransactionStore getTransactionStore() {
            return transactionStore;
        }

        public HashMap<String, MVTable> getTables() {
            return new HashMap<String, MVTable>(tableMap);
        }

        /**
         * Remove a table.
         *
         * @param table the table
         */
        public void removeTable(MVTable table) {
            tableMap.remove(table.getMapName());
        }

        /**
         * Store all pending changes.
         */
        public void flush() {
            FileStore s = store.getFileStore();
            if (s == null || s.isReadOnly()) {
                return;
            }
            if (!store.compact(50)) {
                store.commit();
            }
        }

        /**
         * Close the store, without persisting changes.
         */
        public void closeImmediately() {
            if (store.isClosed()) {
                return;
            }
            store.closeImmediately();
        }

        /**
         * Commit all transactions that are in the committing state, and
         * rollback all open transactions.
         */
        public void initTransactions() {
            List<Transaction> list = transactionStore.getOpenTransactions();
            for (Transaction t : list) {
                if (t.getStatus() == Transaction.STATUS_COMMITTING) {
                    t.commit();
                } else if (t.getStatus() != Transaction.STATUS_PREPARED) {
                    t.rollback();
                }
            }
        }

        /**
         * Remove all temporary maps.
         */
        public void removeTemporaryMaps() {
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith("temp.")) {
                    MVMap<?, ?> map = store.openMap(mapName);
                    store.removeMap(map);
                }
            }
        }

        /**
         * Get the name of the next available temporary map.
         *
         * @return the map name
         */
        public synchronized String nextTemporaryMapName() {
            return "temp." + temporaryMapId++;
        }

        /**
         * Prepare a transaction.
         *
         * @param session the session
         * @param transactionName the transaction name (may be null)
         */
        public void prepareCommit(Session session, String transactionName) {
            Transaction t = session.getTransaction();
            t.setName(transactionName);
            t.prepare();
            store.commit();
        }

        public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
            List<Transaction> list = transactionStore.getOpenTransactions();
            ArrayList<InDoubtTransaction> result = New.arrayList();
            for (Transaction t : list) {
                if (t.getStatus() == Transaction.STATUS_PREPARED) {
                    result.add(new MVInDoubtTransaction(store, t));
                }
            }
            return result;
        }

        public void setCacheSize(int kb) {
            store.setCacheSize(Math.max(1, kb / 1024));
        }

        public InputStream getInputStream() {
            FileChannel fc = store.getFileStore().getEncryptedFile();
            if (fc == null) {
                fc = store.getFileStore().getFile();
            }
            return new FileChannelInputStream(fc, false);
        }

        /**
         * Force the changes to disk.
         */
        public void sync() {
            flush();
            store.sync();
        }

        /**
         * Compact the database file, that is, compact blocks that have a low
         * fill rate, and move chunks next to each other. This will typically
         * shrink the database file. Changes are flushed to the file, and old
         * chunks are overwritten.
         *
         * @param maxCompactTime the maximum time in milliseconds to compact
         */
        public void compactFile(long maxCompactTime) {
            store.setRetentionTime(0);
            long start = System.currentTimeMillis();
            while (store.compact(99)) {
                store.sync();
                long time = System.currentTimeMillis() - start;
                if (time > maxCompactTime) {
                    break;
                }
            }
            store.compactMoveChunks();
        }

        /**
         * Close the store. Pending changes are persisted. Chunks with a low
         * fill rate are compacted, but old chunks are kept for some time, so
         * most likely the database file will not shrink.
         *
         * @param maxCompactTime the maximum time in milliseconds to compact
         */
        public void close(long maxCompactTime) {
            try {
                if (!store.isClosed() && store.getFileStore() != null) {
                    if (!store.getFileStore().isReadOnly()) {
                        transactionStore.close();
                        long start = System.currentTimeMillis();
                        while (store.compact(90)) {
                            long time = System.currentTimeMillis() - start;
                            if (time > maxCompactTime) {
                                break;
                            }
                        }
                    }
                    store.close();
                }
            } catch (IllegalStateException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, "Closing");
            }                
        }

        /**
         * Start collecting statistics.
         */
        public void statisticsStart() {
            FileStore fs = store.getFileStore();
            statisticsStart = fs == null ? 0 : fs.getReadCount();
        }

        /**
         * Stop collecting statistics.
         *
         * @return the statistics
         */
        public Map<String, Integer> statisticsEnd() {
            HashMap<String, Integer> map = New.hashMap();
            FileStore fs = store.getFileStore();
            int reads = fs == null ? 0 : (int) (fs.getReadCount() - statisticsStart);
            map.put("reads", reads);
            return map;
        }

    }

    /**
     * An in-doubt transaction.
     */
    private static class MVInDoubtTransaction implements InDoubtTransaction {

        private final MVStore store;
        private final Transaction transaction;
        private int state = InDoubtTransaction.IN_DOUBT;

        MVInDoubtTransaction(MVStore store, Transaction transaction) {
            this.store = store;
            this.transaction = transaction;
        }

        @Override
        public void setState(int state) {
            if (state == InDoubtTransaction.COMMIT) {
                transaction.commit();
            } else {
                transaction.rollback();
            }
            store.commit();
            this.state = state;
        }

        @Override
        public String getState() {
            switch(state) {
            case IN_DOUBT:
                return "IN_DOUBT";
            case COMMIT:
                return "COMMIT";
            case ROLLBACK:
                return "ROLLBACK";
            default:
                throw DbException.throwInternalError("state="+state);
            }
        }

        @Override
        public String getTransactionName() {
            return transaction.getName();
        }

    }

}
