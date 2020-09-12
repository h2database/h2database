/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.Utils;
import org.h2.value.CompareMode;

/**
 * A table store in a PageStore.
 */
public class PageStoreTable extends RegularTable {

    private Index scanIndex;
    private long rowCount;

    /**
     * The queue of sessions waiting to lock the table. It is a FIFO queue to
     * prevent starvation, since Java's synchronized locking is biased.
     */
    private final ArrayDeque<SessionLocal> waitingSessions = new ArrayDeque<>();
    private final Trace traceLock;
    private final ArrayList<Index> indexes = Utils.newSmallArrayList();
    private long lastModificationId;
    private final PageDataIndex mainIndex;
    private int changesSinceAnalyze;
    private int nextAnalyze;

    public PageStoreTable(CreateTableData data) {
        super(data);
        nextAnalyze = database.getSettings().analyzeAuto;
        if (data.persistData && database.isPersistent()) {
            mainIndex = new PageDataIndex(this, data.id,
                    IndexColumn.wrap(getColumns()),
                    IndexType.createScan(data.persistData),
                    data.create, data.session);
            scanIndex = mainIndex;
        } else {
            mainIndex = null;
            scanIndex = new ScanIndex(this, data.id,
                    IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData));
        }
        indexes.add(scanIndex);
        traceLock = database.getTrace(Trace.LOCK);
    }

    @Override
    public void close(SessionLocal session) {
        for (Index index : indexes) {
            index.close(session);
        }
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        return scanIndex.getRow(session, key);
    }

    @Override
    public void addRow(SessionLocal session, Row row) {
        lastModificationId = database.getNextModificationDataId();
        int i = 0;
        try {
            for (int size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                index.add(session, row);
                checkRowCount(session, index, 1);
            }
            rowCount++;
        } catch (Throwable e) {
            try {
                while (--i >= 0) {
                    Index index = indexes.get(i);
                    index.remove(session, row);
                    checkRowCount(session, index, 0);
                }
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error(e2, "could not undo operation");
                throw e2;
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    private void checkRowCount(SessionLocal session, Index index, int offset) {
        if (SysProperties.CHECK) {
            if (!(index instanceof PageDelegateIndex)) {
                long rc = index.getRowCount(session);
                if (rc != rowCount + offset) {
                    throw DbException.getInternalError("rowCount expected " + (rowCount + offset) + " got " + rc + ' '
                            + getName() + '.' + index.getName());
                }
            }
        }
    }

    @Override
    public Index getScanIndex(SessionLocal session) {
        return indexes.get(0);
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        if (indexType.isSpatial()) {
            throw DbException.getUnsupportedException("MV_STORE=FALSE && SPATIAL INDEX");
        }
        cols = prepareColumns(database, cols, indexType);
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        Index index;
        if (isPersistIndexes() && indexType.isPersistent()) {
            int mainIndexColumn;
            if (database.isStarting() &&
                    database.getPageStore().getRootPageId(indexId) != 0) {
                mainIndexColumn = -1;
            } else if (!database.isStarting() && mainIndex.getRowCount(session) != 0
                    || mainIndex.getMainIndexColumn() != -1) {
                mainIndexColumn = -1;
            } else {
                mainIndexColumn = getMainIndexColumn(indexType, cols);
            }
            if (mainIndexColumn != -1) {
                mainIndex.setMainIndexColumn(mainIndexColumn);
                index = new PageDelegateIndex(this, indexId, indexName,
                        indexType, mainIndex, create, session);
            } else {
                index = new PageBtreeIndex(this, indexId, indexName, cols,
                        indexType, create, session);
            }
        } else {
            if (indexType.isHash()) {
                if (cols.length != 1) {
                    throw DbException.getUnsupportedException(
                            "hash indexes may index only one column");
                }
                if (indexType.isUnique()) {
                    index = new HashIndex(this, indexId, indexName, cols,
                            indexType);
                } else {
                    index = new NonUniqueHashIndex(this, indexId, indexName,
                            cols, indexType);
                }
            } else {
                index = new TreeIndex(this, indexId, indexName, cols, indexType);
            }
        }
        if (index.needRebuild() && rowCount > 0) {
            try {
                Index scan = getScanIndex(session);
                long remaining = scan.getRowCount(session);
                long total = remaining;
                Cursor cursor = scan.find(session, null, null);
                long i = 0;
                int bufferSize = (int) Math.min(rowCount, database.getMaxMemoryRows());
                ArrayList<Row> buffer = new ArrayList<>(bufferSize);
                String n = getName() + ":" + index.getName();
                while (cursor.next()) {
                    database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n, i++, total);
                    Row row = cursor.get();
                    buffer.add(row);
                    if (buffer.size() >= bufferSize) {
                        addRowsToIndex(session, buffer, index);
                    }
                    remaining--;
                }
                addRowsToIndex(session, buffer, index);
                if (remaining != 0) {
                    throw DbException.getInternalError("rowcount remaining=" + remaining + ' ' + getName());
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

    @Override
    public long getRowCount(SessionLocal session) {
        return rowCount;
    }

    @Override
    public void removeRow(SessionLocal session, Row row) {
        lastModificationId = database.getNextModificationDataId();
        int i = indexes.size() - 1;
        try {
            for (; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
                checkRowCount(session, index, -1);
            }
            rowCount--;
        } catch (Throwable e) {
            try {
                while (++i < indexes.size()) {
                    Index index = indexes.get(i);
                    index.add(session, row);
                    checkRowCount(session, index, 0);
                }
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error(e2, "could not undo operation");
                throw e2;
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public long truncate(SessionLocal session) {
        lastModificationId = database.getNextModificationDataId();
        long result = rowCount;
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        rowCount = 0;
        changesSinceAnalyze = 0;
        return result;
    }

    private void analyzeIfRequired(SessionLocal session) {
        if (nextAnalyze == 0 || nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        changesSinceAnalyze = 0;
        int n = 2 * nextAnalyze;
        if (n > 0) {
            nextAnalyze = n;
        }
        session.markTableForAnalyze(this);
    }

    @Override
    public boolean lock(SessionLocal session, boolean exclusive,
            boolean forceLockEvenInMvcc) {
        int lockMode = database.getLockMode();
        if (lockMode == Constants.LOCK_MODE_OFF) {
            return lockExclusiveSession != null;
        }
        if (lockExclusiveSession == session) {
            return true;
        }
        if (!exclusive && lockSharedSessions.containsKey(session)) {
            return true;
        }
        synchronized (database) {
            if (!exclusive && lockSharedSessions.contains(session)) {
                return true;
            }
            session.setWaitForLock(this, Thread.currentThread());
            waitingSessions.addLast(session);
            try {
                doLock1(session, lockMode, exclusive);
            } finally {
                session.setWaitForLock(null, null);
                waitingSessions.remove(session);
            }
        }
        return false;
    }

    private void doLock1(SessionLocal session, int lockMode, boolean exclusive) {
        traceLock(session, exclusive, "requesting for");
        // don't get the current time unless necessary
        long max = 0;
        boolean checkDeadlock = false;
        while (true) {
            // if I'm the next one in the queue
            if (waitingSessions.getFirst() == session) {
                if (doLock2(session, lockMode, exclusive)) {
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
                traceLock(session, exclusive, "timeout after " + session.getLockTimeout());
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, getName());
            }
            try {
                traceLock(session, exclusive, "waiting for");
                if (database.getLockMode() == Constants.LOCK_MODE_TABLE_GC) {
                    for (int i = 0; i < 20; i++) {
                        long free = Runtime.getRuntime().freeMemory();
                        System.gc();
                        long free2 = Runtime.getRuntime().freeMemory();
                        if (free == free2) {
                            break;
                        }
                    }
                }
                // don't wait too long so that deadlocks are detected early
                long sleep = Math.min(Constants.DEADLOCK_CHECK, (max - now) / 1_000_000);
                if (sleep == 0) {
                    sleep = 1;
                }
                database.wait(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private boolean doLock2(SessionLocal session, int lockMode, boolean exclusive) {
        if (exclusive) {
            if (lockExclusiveSession == null) {
                if (lockSharedSessions.isEmpty()) {
                    traceLock(session, exclusive, "added for");
                    session.registerTableAsLocked(this);
                    lockExclusiveSession = session;
                    return true;
                } else if (lockSharedSessions.size() == 1 &&
                        lockSharedSessions.containsKey(session)) {
                    traceLock(session, exclusive, "add (upgraded) for ");
                    lockExclusiveSession = session;
                    return true;
                }
            }
        } else {
            if (lockExclusiveSession == null) {
                if (lockMode == Constants.LOCK_MODE_READ_COMMITTED) {
                    // PageStore is single-threaded, no lock is required
                    return true;
                }
                if (!lockSharedSessions.containsKey(session)) {
                    traceLock(session, exclusive, "ok");
                    session.registerTableAsLocked(this);
                    lockSharedSessions.put(session, session);
                }
                return true;
            }
        }
        return false;
    }

    private void traceLock(SessionLocal session, boolean exclusive, String s) {
        if (traceLock.isDebugEnabled()) {
            traceLock.debug("{0} {1} {2} {3}", session.getId(),
                    exclusive ? "exclusive write lock" : "shared read lock", s, getName());
        }
    }

    @Override
    public void unlock(SessionLocal s) {
        if (database != null) {
            traceLock(s, lockExclusiveSession == s, "unlock");
            if (lockExclusiveSession == s) {
                lockSharedSessions.remove(s);
                lockExclusiveSession = null;
            }
            synchronized (database) {
                if (!lockSharedSessions.isEmpty()) {
                    lockSharedSessions.remove(s);
                }
                if (!waitingSessions.isEmpty()) {
                    database.notifyAll();
                }
            }
        }
    }

    /**
     * Set the row count of this table.
     *
     * @param count the row count
     */
    public void setRowCount(long count) {
        this.rowCount = count;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        if (containsLargeObject) {
            // unfortunately, the data is gone on rollback
            truncate(session);
            database.getLobStorage().removeAllForTable(getId());
            database.lockMeta(session);
        }
        super.removeChildrenAndResources(session);
        // go backwards because database.removeIndex will call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
            // needed for session temporary indexes
            indexes.remove(index);
        }
        scanIndex.remove(session);
        database.removeMeta(session, getId());
        scanIndex = null;
        lockExclusiveSession = null;
        lockSharedSessions.clear();
        invalidate();
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return scanIndex.getRowCountApproximation(session);
    }

    @Override
    public long getDiskSpaceUsed() {
        return scanIndex.getDiskSpaceUsed();
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    @Override
    public int getMainIndexColumn() {
        return mainIndex != null ? mainIndex.getMainIndexColumn() : SearchRow.ROWID_INDEX;
    }

}
