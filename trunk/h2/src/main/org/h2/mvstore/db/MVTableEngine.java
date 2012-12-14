/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreBuilder;
import org.h2.mvstore.type.DataTypeFactory;
import org.h2.table.TableBase;
import org.h2.util.New;

/**
 * A table engine that internally uses the MVStore.
 */
public class MVTableEngine implements TableEngine {

    static final Map<String, Store> STORES = new WeakHashMap<String, Store>();

    public static void flush(Database db) {
        String storeName = db.getDatabasePath();
        if (storeName == null) {
            return;
        }
        synchronized (STORES) {
            Store store = STORES.get(storeName);
            if (store == null) {
                return;
            }
            // TODO this stores uncommitted transactions as well
            store.store.store();
        }
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        String storeName = db.getDatabasePath();
        MVStoreBuilder storeBuilder;
        Store store;
        DataTypeFactory f = new ValueDataTypeFactory(db.getCompareMode(), db);
        if (storeName == null) {
            storeBuilder = MVStoreBuilder.inMemory();
            storeBuilder.with(f);
            store = new Store(db, storeBuilder.open());
        } else {
            synchronized (STORES) {
                store = STORES.get(storeName);
                if (store == null) {
                    storeBuilder = MVStoreBuilder.fileBased(storeName + Constants.SUFFIX_MV_FILE);
                    storeBuilder.with(f);
                    store = new Store(db, storeBuilder.open());
                    STORES.put(storeName, store);
                } else if (store.db != db) {
                    throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, storeName);
                }
            }
        }
        MVTable table = new MVTable(data, storeName, store.store);
        store.openTables.add(table);
        table.init(data.session);
        return table;
    }

    static void closeTable(String storeName, MVTable table) {
        synchronized (STORES) {
            Store store = STORES.get(storeName);
            if (store != null) {
                store.openTables.remove(table);
                if (store.openTables.size() == 0) {
                    store.store.store();
                    store.store.close();
                    STORES.remove(storeName);
                }
            }
        }
    }

    /**
     * A store with open tables.
     */
    static class Store {

        final Database db;
        final MVStore store;
        final ArrayList<MVTable> openTables = New.arrayList();

        public Store(Database db, MVStore store) {
            this.db = db;
            this.store = store;
        }

    }

}
