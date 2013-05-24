/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.table.TableBase;
import org.h2.util.New;

/**
 * A table engine that internally uses the MVStore.
 */
public class MVTableEngine implements TableEngine {

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        Store store = db.getMvStore();
        if (store == null) {
            byte[] key = db.getFilePasswordHash();
            String dbPath = db.getDatabasePath();
            MVStore.Builder builder = new MVStore.Builder();
            if (dbPath == null) {
                store = new Store(db, builder.open());
            } else {
                builder.fileName(dbPath + Constants.SUFFIX_MV_FILE);
                if (db.isReadOnly()) {
                    builder.readOnly();
                }
                if (key != null) {
                    char[] password = new char[key.length];
                    for (int i = 0; i < key.length; i++) {
                        password[i] = (char) key[i];
                    }
                    builder.encryptionKey(password);
                }
                store = new Store(db, builder.open());
            }
            db.setMvStore(store);
        }
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
                    new ValueDataType(null, null, null));
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
            FileChannel f = store.getFile();
            if (f != null) {
                try {
                    f.close();
                } catch (IOException e) {
                    throw DbException.convertIOException(e, "Closing file");
                }
            }
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

    }

}
