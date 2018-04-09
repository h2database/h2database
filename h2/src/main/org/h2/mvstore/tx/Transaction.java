/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.DataType;
import java.util.Iterator;

/**
 * A transaction.
 */
public class Transaction {

    /**
     * The status of a closed transaction (committed or rolled back).
     */
    public static final int STATUS_CLOSED = 0;

    /**
     * The status of an open transaction.
     */
    public static final int STATUS_OPEN = 1;

    /**
     * The status of a prepared transaction.
     */
    public static final int STATUS_PREPARED = 2;

    /**
     * The status of a transaction that is being committed, but possibly not
     * yet finished. A transactions can go into this state when the store is
     * closed while the transaction is committing. When opening a store,
     * such transactions should be committed.
     */
    public static final int STATUS_COMMITTING = 3;

    /**
     * The transaction store.
     */
    final TransactionStore store;

    /**
     * The transaction id.
     */
    final int transactionId;

    /**
     * The log id of the last entry in the undo log map.
     */
    long logId;

    private int status;

    private MVStore.TxCounter txCounter;

    private String name;

    Transaction(TransactionStore store, int transactionId, int status,
                String name, long logId) {
        this.store = store;
        this.transactionId = transactionId;
        this.status = status;
        this.name = name;
        this.logId = logId;
    }

    public int getId() {
        return transactionId;
    }

    public int getStatus() {
        return status;
    }

    void setStatus(int status) {
        this.status = status;
    }

    public void setName(String name) {
        checkNotClosed();
        this.name = name;
        store.storeTransaction(this);
    }

    public String getName() {
        return name;
    }

    /**
     * Create a new savepoint.
     *
     * @return the savepoint id
     */
    public long setSavepoint() {
        return logId;
    }

    public void markStatementStart() {
        markStatementEnd();
        txCounter = store.store.registerVersionUsage();
    }

    public void markStatementEnd() {
        MVStore.TxCounter counter = txCounter;
        txCounter = null;
        if(counter != null) {
            store.store.deregisterVersionUsage(counter);
        }
    }

    /**
     * Add a log entry.
     *
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    void log(int mapId, Object key, Object oldValue) {
        store.log(this, logId, mapId, key, oldValue);
        // only increment the log id if logging was successful
        logId++;
    }

    /**
     * Remove the last log entry.
     */
    void logUndo() {
        store.logUndo(this, --logId);
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
        return openMap(name, null, null);
    }

    /**
     * Open the map to store the data.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param keyType the key data type
     * @param valueType the value data type
     * @return the transaction map
     */
    public <K, V> TransactionMap<K, V> openMap(String name,
                                               DataType keyType, DataType valueType) {
        checkNotClosed();
        MVMap<K, VersionedValue> map = store.openMap(name, keyType,
                valueType);
        int mapId = map.getId();
        return new TransactionMap<>(this, map, mapId);
    }

    /**
     * Open the transactional version of the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the base map
     * @return the transactional map
     */
    public <K, V> TransactionMap<K, V> openMap(
            MVMap<K, VersionedValue> map) {
        checkNotClosed();
        int mapId = map.getId();
        return new TransactionMap<>(this, map, mapId);
    }

    /**
     * Prepare the transaction. Afterwards, the transaction can only be
     * committed or rolled back.
     */
    public void prepare() {
        checkNotClosed();
        status = STATUS_PREPARED;
        store.storeTransaction(this);
    }

    /**
     * Commit the transaction. Afterwards, this transaction is closed.
     */
    public void commit() {
        checkNotClosed();
        store.commit(this, logId);
    }

    /**
     * Roll back to the given savepoint. This is only allowed if the
     * transaction is open.
     *
     * @param savepointId the savepoint id
     */
    public void rollbackToSavepoint(long savepointId) {
        checkNotClosed();
        store.rollbackTo(this, logId, savepointId);
        logId = savepointId;
    }

    /**
     * Roll the transaction back. Afterwards, this transaction is closed.
     */
    public void rollback() {
        checkNotClosed();
        store.rollbackTo(this, logId, 0);
        store.endTransaction(this, status);
    }

    /**
     * Get the list of changes, starting with the latest change, up to the
     * given savepoint (in reverse order than they occurred). The value of
     * the change is the value before the change was applied.
     *
     * @param savepointId the savepoint id, 0 meaning the beginning of the
     *            transaction
     * @return the changes
     */
    public Iterator<TransactionStore.Change> getChanges(long savepointId) {
        return store.getChanges(this, logId, savepointId);
    }

    /**
     * Check whether this transaction is open or prepared.
     */
    void checkNotClosed() {
        if (status == STATUS_CLOSED) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_CLOSED, "Transaction is closed");
        }
    }

    /**
     * Remove the map.
     *
     * @param map the map
     */
    public <K, V> void removeMap(TransactionMap<K, V> map) {
        store.removeMap(map);
    }

    @Override
    public String toString() {
        return "" + transactionId;
    }

}
