/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.engine.SysProperties;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.DebuggingThreadLocal;
import org.h2.util.Utils;

/**
 * A table stored in a MVStore.
 */
public class MVTable extends RegularTable {
    /**
     * The table name this thread is waiting to lock.
     */
    public static final DebuggingThreadLocal<String> WAITING_FOR_LOCK;

    /**
     * The table names this thread has exclusively locked.
     */
    public static final DebuggingThreadLocal<ArrayList<String>> EXCLUSIVE_LOCKS;

    /**
     * The tables names this thread has a shared lock on.
     */
    public static final DebuggingThreadLocal<ArrayList<String>> SHARED_LOCKS;

    /**
     * The type of trace lock events
     */
    private enum TraceLockEvent{

        TRACE_LOCK_OK("ok"),
        TRACE_LOCK_WAITING_FOR("waiting for"),
        TRACE_LOCK_REQUESTING_FOR("requesting for"),
        TRACE_LOCK_TIMEOUT_AFTER("timeout after "),
        TRACE_LOCK_UNLOCK("unlock"),
        TRACE_LOCK_ADDED_FOR("added for"),
        TRACE_LOCK_ADD_UPGRADED_FOR("add (upgraded) for ");

        private final String eventText;

        TraceLockEvent(String eventText) {
            this.eventText = eventText;
        }

        public String getEventText() {
            return eventText;
        }
    }
    private static final String NO_EXTRA_INFO = "";

    static {
        if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
            WAITING_FOR_LOCK = new DebuggingThreadLocal<>();
            EXCLUSIVE_LOCKS = new DebuggingThreadLocal<>();
            SHARED_LOCKS = new DebuggingThreadLocal<>();
        } else {
            WAITING_FOR_LOCK = null;
            EXCLUSIVE_LOCKS = null;
            SHARED_LOCKS = null;
        }
    }

    private MVPrimaryIndex primaryIndex;
    private final ArrayList<Index> indexes = Utils.newSmallArrayList();
    private final AtomicLong lastModificationId = new AtomicLong();

    /**
     * The queue of sessions waiting to lock the table. It is a FIFO queue to
     * prevent starvation, since Java's synchronized locking is biased.
     */
    private final ArrayDeque<SessionLocal> waitingSessions = new ArrayDeque<>();
    private final Trace traceLock;
    private final AtomicInteger changesUntilAnalyze;
    private int nextAnalyze;

    private final Store store;
    private final TransactionStore transactionStore;

    public MVTable(CreateTableData data, Store store) {
        super(data);
        nextAnalyze = database.getSettings().analyzeAuto;
        changesUntilAnalyze = nextAnalyze <= 0 ? null : new AtomicInteger(nextAnalyze);
        this.store = store;
        this.transactionStore = store.getTransactionStore();
        traceLock = database.getTrace(Trace.LOCK);

        primaryIndex = new MVPrimaryIndex(database, this, getId(),
                IndexColumn.wrap(getColumns()), IndexType.createScan(true));
        indexes.add(primaryIndex);
    }

    public String getMapName() {
        return primaryIndex.getMapName();
    }

    @Override
    public boolean lock(SessionLocal session, boolean exclusive,
            boolean forceLockEvenInMvcc) {
        int lockMode = database.getLockMode();
        if (lockMode == Constants.LOCK_MODE_OFF) {
            session.registerTableAsUpdated(this);
            return false;
        }
        if (!forceLockEvenInMvcc) {
            // MVCC: update, delete, and insert use a shared lock.
            // Select doesn't lock except when using FOR UPDATE and
            // the system property h2.selectForUpdateMvcc
            // is not enabled
            if (exclusive) {
                exclusive = false;
            } else {
                if (lockExclusiveSession == null) {
                    return false;
                }
            }
        }
        if (lockExclusiveSession == session) {
            return true;
        }
        if (!exclusive && lockSharedSessions.containsKey(session)) {
            return true;
        }
        synchronized (this) {
            if (!exclusive && lockSharedSessions.containsKey(session)) {
                return true;
            }
            session.setWaitForLock(this, Thread.currentThread());
            if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                WAITING_FOR_LOCK.set(getName());
            }
            waitingSessions.addLast(session);
            try {
                doLock1(session, exclusive);
            } finally {
                session.setWaitForLock(null, null);
                if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                    WAITING_FOR_LOCK.remove();
                }
                waitingSessions.remove(session);
            }
        }
        return false;
    }

    private void doLock1(SessionLocal session, boolean exclusive) {
        traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_REQUESTING_FOR, NO_EXTRA_INFO);
        // don't get the current time unless necessary
        long max = 0L;
        boolean checkDeadlock = false;
        while (true) {
            // if I'm the next one in the queue
            if (waitingSessions.getFirst() == session) {
                if (doLock2(session, exclusive)) {
                    return;
                }
            }
            if (checkDeadlock) {
                ArrayList<SessionLocal> sessions = checkDeadlock(session, null, null);
                if (sessions != null) {
                    throw DbException.get(ErrorCode.DEADLOCK_1,
                            getDeadlockDetails(sessions, exclusive));
                }
            } else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }
            long now = System.nanoTime();
            if (max == 0L) {
                // try at least one more time
                max = Utils.nanoTimePlusMillis(now, session.getLockTimeout());
            } else if (now - max >= 0L) {
                traceLock(session, exclusive,
                        TraceLockEvent.TRACE_LOCK_TIMEOUT_AFTER, NO_EXTRA_INFO+session.getLockTimeout());
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, getName());
            }
            try {
                traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_WAITING_FOR, NO_EXTRA_INFO);
                // don't wait too long so that deadlocks are detected early
                long sleep = Math.min(Constants.DEADLOCK_CHECK, (max - now) / 1_000_000L);
                if (sleep == 0) {
                    sleep = 1;
                }
                wait(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private boolean doLock2(SessionLocal session, boolean exclusive) {
        if (lockExclusiveSession == null) {
            if (exclusive) {
                if (lockSharedSessions.isEmpty()) {
                    traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_ADDED_FOR, NO_EXTRA_INFO);
                    session.registerTableAsLocked(this);
                    lockExclusiveSession = session;
                    if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                        if (EXCLUSIVE_LOCKS.get() == null) {
                            EXCLUSIVE_LOCKS.set(new ArrayList<String>());
                        }
                        EXCLUSIVE_LOCKS.get().add(getName());
                    }
                    return true;
                } else if (lockSharedSessions.size() == 1 &&
                        lockSharedSessions.containsKey(session)) {
                    traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_ADD_UPGRADED_FOR, NO_EXTRA_INFO);
                    lockExclusiveSession = session;
                    if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                        if (EXCLUSIVE_LOCKS.get() == null) {
                            EXCLUSIVE_LOCKS.set(new ArrayList<String>());
                        }
                        EXCLUSIVE_LOCKS.get().add(getName());
                    }
                    return true;
                }
            } else {
                if (lockSharedSessions.putIfAbsent(session, session) == null) {
                    traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_OK, NO_EXTRA_INFO);
                    session.registerTableAsLocked(this);
                    if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                        ArrayList<String> list = SHARED_LOCKS.get();
                        if (list == null) {
                            list = new ArrayList<>();
                            SHARED_LOCKS.set(list);
                        }
                        list.add(getName());
                    }
                }
                return true;
            }
        }
        return false;
    }

    private void traceLock(SessionLocal session, boolean exclusive, TraceLockEvent eventEnum, String extraInfo) {
        if (traceLock.isDebugEnabled()) {
            traceLock.debug("{0} {1} {2} {3} {4}", session.getId(),
                    exclusive ? "exclusive write lock" : "shared read lock", eventEnum.getEventText(),
                    getName(), extraInfo);
        }
    }

    @Override
    public void unlock(SessionLocal s) {
        if (database != null) {
            boolean wasLocked = lockExclusiveSession == s;
            traceLock(s, wasLocked, TraceLockEvent.TRACE_LOCK_UNLOCK, NO_EXTRA_INFO);
            if (wasLocked) {
                lockSharedSessions.remove(s);
                lockExclusiveSession = null;
                if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                    if (EXCLUSIVE_LOCKS.get() != null) {
                        EXCLUSIVE_LOCKS.get().remove(getName());
                    }
                }
            } else {
                wasLocked = lockSharedSessions.remove(s) != null;
                if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                    if (SHARED_LOCKS.get() != null) {
                        SHARED_LOCKS.get().remove(getName());
                    }
                }
            }
            if (wasLocked && !waitingSessions.isEmpty()) {
                synchronized (this) {
                    notifyAll();
                }
            }
        }
    }

    @Override
    public void close(SessionLocal session) {
        // ignore
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        return primaryIndex.getRow(session, key);
    }

    @Override
    public Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
        cols = prepareColumns(database, cols, indexType);
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        MVIndex<?,?> index;
        int mainIndexColumn = primaryIndex.getMainIndexColumn() != SearchRow.ROWID_INDEX
                ? SearchRow.ROWID_INDEX : getMainIndexColumn(indexType, cols);
        if (database.isStarting()) {
            // if index does exists as a separate map it can't be a delegate
            if (transactionStore.hasMap("index." + indexId)) {
                // we can not reuse primary index
                mainIndexColumn = SearchRow.ROWID_INDEX;
            }
        } else if (primaryIndex.getRowCountMax() != 0) {
            mainIndexColumn = SearchRow.ROWID_INDEX;
        }

        if (mainIndexColumn != SearchRow.ROWID_INDEX) {
            primaryIndex.setMainIndexColumn(mainIndexColumn);
            index = new MVDelegateIndex(this, indexId, indexName, primaryIndex,
                    indexType);
        } else if (indexType.isSpatial()) {
            index = new MVSpatialIndex(session.getDatabase(), this, indexId,
                    indexName, cols, uniqueColumnCount, indexType);
        } else {
            index = new MVSecondaryIndex(session.getDatabase(), this, indexId,
                    indexName, cols, uniqueColumnCount, indexType);
        }
        if (index.needRebuild()) {
            rebuildIndex(session, index, indexName);
        }
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    private void rebuildIndex(SessionLocal session, MVIndex<?,?> index, String indexName) {
        try {
            if (session.getDatabase().getStore() == null || index instanceof MVSpatialIndex) {
                // in-memory
                rebuildIndexBuffered(session, index);
            } else {
                rebuildIndexBlockMerge(session, index);
            }
        } catch (DbException e) {
            getSchema().freeUniqueName(indexName);
            try {
                index.remove(session);
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means
                // there is something wrong with the database
                trace.error(e2, "could not remove index");
                throw e2;
            }
            throw e;
        }
    }

    private void rebuildIndexBlockMerge(SessionLocal session, MVIndex<?,?> index) {
        // Read entries in memory, sort them, write to a new map (in sorted
        // order); repeat (using a new map for every block of 1 MB) until all
        // record are read. Merge all maps to the target (using merge sort;
        // duplicates are detected in the target). For randomly ordered data,
        // this should use relatively few write operations.
        // A possible optimization is: change the buffer size from "row count"
        // to "amount of memory", and buffer index keys instead of rows.
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        Store store = session.getDatabase().getStore();

        int bufferSize = database.getMaxMemoryRows() / 2;
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ':' + index.getName();
        ArrayList<String> bufferNames = Utils.newSmallArrayList();
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, i++, total);
            if (buffer.size() >= bufferSize) {
                sortRows(buffer, index);
                String mapName = store.nextTemporaryMapName();
                index.addRowsToBuffer(buffer, mapName);
                bufferNames.add(mapName);
                buffer.clear();
            }
            remaining--;
        }
        sortRows(buffer, index);
        if (!bufferNames.isEmpty()) {
            String mapName = store.nextTemporaryMapName();
            index.addRowsToBuffer(buffer, mapName);
            bufferNames.add(mapName);
            buffer.clear();
            index.addBufferedRows(bufferNames);
        } else {
            addRowsToIndex(session, buffer, index);
        }
        if (remaining != 0) {
            throw DbException.getInternalError("rowcount remaining=" + remaining + ' ' + getName());
        }
    }

    private void rebuildIndexBuffered(SessionLocal session, Index index) {
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        int bufferSize = (int) Math.min(total, database.getMaxMemoryRows());
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ':' + index.getName();
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, i++, total);
            if (buffer.size() >= bufferSize) {
                addRowsToIndex(session, buffer, index);
            }
            remaining--;
        }
        addRowsToIndex(session, buffer, index);
        if (remaining != 0) {
            throw DbException.getInternalError("rowcount remaining=" + remaining + ' ' + getName());
        }
    }

    @Override
    public void removeRow(SessionLocal session, Row row) {
        syncLastModificationIdWithDatabase();
        Transaction t = session.getTransaction();
        long savepoint = t.setSavepoint();
        try {
            for (int i = indexes.size() - 1; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
            }
        } catch (Throwable e) {
            try {
                t.rollbackToSavepoint(savepoint);
            } catch (Throwable nested) {
                e.addSuppressed(nested);
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public long truncate(SessionLocal session) {
        syncLastModificationIdWithDatabase();
        long result = getRowCountApproximation(session);
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        if (changesUntilAnalyze != null) {
            changesUntilAnalyze.set(nextAnalyze);
        }
        return result;
    }

    @Override
    public void addRow(SessionLocal session, Row row) {
        syncLastModificationIdWithDatabase();
        Transaction t = session.getTransaction();
        long savepoint = t.setSavepoint();
        try {
            for (Index index : indexes) {
                index.add(session, row);
            }
        } catch (Throwable e) {
            try {
                t.rollbackToSavepoint(savepoint);
            } catch (Throwable nested) {
                e.addSuppressed(nested);
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public void updateRow(SessionLocal session, Row oldRow, Row newRow) {
        newRow.setKey(oldRow.getKey());
        syncLastModificationIdWithDatabase();
        Transaction t = session.getTransaction();
        long savepoint = t.setSavepoint();
        try {
            for (Index index : indexes) {
                index.update(session, oldRow, newRow);
            }
        } catch (Throwable e) {
            try {
                t.rollbackToSavepoint(savepoint);
            } catch (Throwable nested) {
                e.addSuppressed(nested);
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public Row lockRow(SessionLocal session, Row row) {
        Row lockedRow = primaryIndex.lockRow(session, row);
        if (lockedRow == null || !row.hasSharedData(lockedRow)) {
            syncLastModificationIdWithDatabase();
        }
        return lockedRow;
    }

    private void analyzeIfRequired(SessionLocal session) {
        if (changesUntilAnalyze != null) {
            if (changesUntilAnalyze.decrementAndGet() == 0) {
                if (nextAnalyze <= Integer.MAX_VALUE / 2) {
                    nextAnalyze *= 2;
                }
                changesUntilAnalyze.set(nextAnalyze);
                session.markTableForAnalyze(this);
            }
        }
    }

    @Override
    public Index getScanIndex(SessionLocal session) {
        return primaryIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId.get();
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        if (containsLargeObject) {
            // unfortunately, the data is gone on rollback
            truncate(session);
            database.getLobStorage().removeAllForTable(getId());
            database.lockMeta(session);
        }
        database.getStore().removeTable(this);
        super.removeChildrenAndResources(session);
        // remove scan index (at position 0 on the list) last
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            index.remove(session);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
            // needed for session temporary indexes
            indexes.remove(index);
        }
        primaryIndex.remove(session);
        indexes.clear();
        close(session);
        invalidate();
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return primaryIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return primaryIndex.getRowCountApproximation(session);
    }

    @Override
    public long getDiskSpaceUsed() {
        return primaryIndex.getDiskSpaceUsed();
    }

    /**
     * Get a new transaction.
     *
     * @return the transaction
     */
    Transaction getTransactionBegin() {
        // TODO need to commit/rollback the transaction
        return transactionStore.begin();
    }

    @Override
    public boolean isMVStore() {
        return true;
    }

    /**
     * Mark the transaction as committed, so that the modification counter of
     * the database is incremented.
     */
    public void commit() {
        if (database != null) {
            syncLastModificationIdWithDatabase();
        }
    }

    // Field lastModificationId can not be just a volatile, because window of opportunity
    // between reading database's modification id and storing this value in the field
    // could be exploited by another thread.
    // Second thread may do the same with possibly bigger (already advanced)
    // modification id, and when first thread finally updates the field, it will
    // result in lastModificationId jumping back.
    // This is, of course, unacceptable.
    private void syncLastModificationIdWithDatabase() {
        long nextModificationDataId = database.getNextModificationDataId();
        long currentId;
        do {
            currentId = lastModificationId.get();
        } while (nextModificationDataId > currentId &&
                !lastModificationId.compareAndSet(currentId, nextModificationDataId));
    }

    /**
     * Convert the MVStoreException to a database exception.
     *
     * @param e the illegal state exception
     * @return the database exception
     */
    DbException convertException(MVStoreException e) {
        int errorCode = e.getErrorCode();
        if (errorCode == DataUtils.ERROR_TRANSACTION_LOCKED) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1,
                    e, getName());
        }
        if (errorCode == DataUtils.ERROR_TRANSACTIONS_DEADLOCK) {
            throw DbException.get(ErrorCode.DEADLOCK_1,
                    e, getName());
        }
        return store.convertMVStoreException(e);
    }

    @Override
    public int getMainIndexColumn() {
        return primaryIndex.getMainIndexColumn();
    }

}
