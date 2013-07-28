/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
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
    public static Store init(Database db) {
        Store store = db.getMvStore();
        if (store == null) {
            byte[] key = db.getFilePasswordHash();
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
                    char[] password = new char[key.length];
                    for (int i = 0; i < key.length; i++) {
                        password[i] = (char) key[i];
                    }
                    builder.encryptionKey(password);
                }
                try {
                    store = new Store(db, builder.open());
                } catch (IllegalStateException e) {
                    int errorCode = DataUtils.getErrorCode(e.getMessage());
                    if (errorCode == DataUtils.ERROR_FILE_CORRUPT) {
                        if (key != null) {
                            throw DbException.get(ErrorCode.FILE_ENCRYPTION_ERROR_1, fileName);
                        }
                    } else if (errorCode == DataUtils.ERROR_FILE_LOCKED) {
                        throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, fileName);
                    }
                    throw DbException.get(ErrorCode.FILE_CORRUPTED_1, fileName);
                }
            }
            db.setMvStore(store);
        }
        return store;
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        if (!data.persistData || (data.temporary && !data.persistIndexes)) {
            return new RegularTable(data);
        }
        Store store = init(db);
        MVTable table = new MVTable(data, store);
        store.openTables.add(table);
        table.init(data.session);
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
         * The list of open tables.
         */
        final ArrayList<MVTable> openTables = New.arrayList();

        /**
         * The store.
         */
        private final MVStore store;

        /**
         * The transaction store.
         */
        private final TransactionStore transactionStore;

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

        public List<MVTable> getTables() {
            return openTables;
        }

        /**
         * Remove a table.
         *
         * @param table the table
         */
        public void removeTable(MVTable table) {
            openTables.remove(table);
        }

        /**
         * Store all pending changes.
         */
        public void store() {
            if (!store.isReadOnly()) {
                store.commit();
                store.compact(50);
                store.store();
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
         * Close the store. Pending changes are persisted.
         */
        public void close() {
            if (!store.isClosed()) {
                if (!store.isReadOnly()) {
                    store.store();
                }
                store.close();
            }
        }

        public void setWriteDelay(int value) {
            store.setWriteDelay(value);
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
         * Prepare a transaction.
         *
         * @param session the session
         * @param transactionName the transaction name (may be null)
         */
        public void prepareCommit(Session session, String transactionName) {
            Transaction t = session.getTransaction();
            t.setName(transactionName);
            t.prepare();
            store.store();
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
            store.setCacheSize(kb * 1024);
        }

        public InputStream getInputStream() {
            return new FileChannelInputStream(store.getFile(), false);
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
            store.store();
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
