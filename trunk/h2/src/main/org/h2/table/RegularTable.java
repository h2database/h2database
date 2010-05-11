/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.h2.api.DatabaseEventListener;
import org.h2.command.ddl.Analyze;
import org.h2.command.ddl.CreateTableData;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.HashIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MultiVersionIndex;
import org.h2.index.NonUniqueHashIndex;
import org.h2.index.PageBtreeIndex;
import org.h2.index.PageDataIndex;
import org.h2.index.PageDelegateIndex;
import org.h2.index.ScanIndex;
import org.h2.index.TreeIndex;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObject;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Most tables are an instance of this class. For this table, the data is stored
 * in the database. The actual data is not kept here, instead it is kept in the
 * indexes. There is at least one index, the scan index.
 */
public class RegularTable extends TableBase {
    private Index scanIndex;
    private long rowCount;
    private volatile Session lockExclusive;
    private HashSet<Session> lockShared = New.hashSet();
    private Trace traceLock;
    private final ArrayList<Index> indexes = New.arrayList();
    private long lastModificationId;
    private boolean containsLargeObject;
    private PageDataIndex mainIndex;
    private int changesSinceAnalyze;
    private int nextAnalyze = SysProperties.ANALYZE_AUTO;

    /**
     * True if one thread ever was waiting to lock this table. This is to avoid
     * calling notifyAll if no session was ever waiting to lock this table. If
     * set, the flag stays. In theory, it could be reset, however not sure when.
     */
    private boolean waitForLock;

    public RegularTable(CreateTableData data) {
        super(data);
        this.isHidden = data.isHidden;
        if (data.persistData && database.isPersistent()) {
            mainIndex = new PageDataIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData), data.create, data.session);
            scanIndex = mainIndex;
        } else {
            scanIndex = new ScanIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData));
        }
        indexes.add(scanIndex);
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType())) {
                containsLargeObject = true;
                memoryPerRow = Row.MEMORY_CALCULATE;
            }
        }
        traceLock = database.getTrace(Trace.LOCK);
    }

    public void close(Session session) {
        for (Index index : indexes) {
            index.close(session);
        }
    }

    /**
     * Read the given row.
     *
     * @param session the session
     * @param key unique key
     * @return the row
     */
    public Row getRow(Session session, long key) {
        return scanIndex.getRow(session, key);
    }

    public void addRow(Session session, Row row) {
        int i = 0;
        lastModificationId = database.getNextModificationDataId();
        if (database.isMultiVersion()) {
            row.setSessionId(session.getId());
        }
        try {
            for (; i < indexes.size(); i++) {
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
                trace.error("Could not undo operation", e);
                throw e2;
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    private void checkRowCount(Session session, Index index, int offset) {
        if (SysProperties.CHECK && !database.isMultiVersion()) {
            if (!(index instanceof PageDelegateIndex)) {
                long rc = index.getRowCount(session);
                if (rc != rowCount + offset) {
                    DbException.throwInternalError("rowCount expected " + (rowCount + offset) + " got " + rc + " " + getName() + "." + index.getName());
                }
            }
        }
    }

    public Index getScanIndex(Session session) {
        return indexes.get(0);
    }

    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        Index index;
        if (isPersistIndexes() && indexType.isPersistent()) {
            int mainIndexColumn;
            if (database.isStarting() && database.getPageStore().getRootPageId(indexId) != 0) {
                mainIndexColumn = -1;
            } else if (!database.isStarting() && mainIndex.getRowCount(session) != 0) {
                mainIndexColumn = -1;
            } else {
                mainIndexColumn = getMainIndexColumn(indexType, cols);
            }
            if (mainIndexColumn != -1) {
                mainIndex.setMainIndexColumn(mainIndexColumn);
                index = new PageDelegateIndex(this, indexId, indexName, indexType, mainIndex, create, session);
            } else {
                index = new PageBtreeIndex(this, indexId, indexName, cols, indexType, create, session);
            }
        } else {
            if (indexType.isHash() && cols.length <= 1) {
                if (indexType.isUnique()) {
                    index = new HashIndex(this, indexId, indexName, cols, indexType);
                } else {
                    index = new NonUniqueHashIndex(this, indexId, indexName, cols, indexType);
                }
            } else {
                index = new TreeIndex(this, indexId, indexName, cols, indexType);
            }
        }
        if (database.isMultiVersion()) {
            index = new MultiVersionIndex(index, this);
        }
        if (index.needRebuild() && rowCount > 0) {
            try {
                Index scan = getScanIndex(session);
                long remaining = scan.getRowCount(session);
                long total = remaining;
                Cursor cursor = scan.find(session, null, null);
                long i = 0;
                int bufferSize = Constants.DEFAULT_MAX_MEMORY_ROWS;
                ArrayList<Row> buffer = New.arrayList(bufferSize);
                String n = getName() + ":" + index.getName();
                int t = MathUtils.convertLongToInt(total);
                while (cursor.next()) {
                    database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                            MathUtils.convertLongToInt(i++), t);
                    Row row = cursor.get();
                    buffer.add(row);
                    if (buffer.size() >= bufferSize) {
                        addRowsToIndex(session, buffer, index);
                    }
                    remaining--;
                }
                addRowsToIndex(session, buffer, index);
                if (SysProperties.CHECK && remaining != 0) {
                    DbException.throwInternalError("rowcount remaining=" + remaining + " " + getName());
                }
            } catch (DbException e) {
                getSchema().freeUniqueName(indexName);
                try {
                    index.remove(session);
                } catch (DbException e2) {
                    // this could happen, for example on failure in the storage
                    // but if that is not the case it means
                    // there is something wrong with the database
                    trace.error("Could not remove index", e);
                    throw e2;
                }
                throw e;
            }
        }
        boolean temporary = isTemporary();
        index.setTemporary(temporary);
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (temporary && !isGlobalTemporary()) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    private int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (mainIndex.getMainIndexColumn() != -1) {
            return -1;
        }
        if (!indexType.isPrimaryKey() || cols.length != 1) {
            return -1;
        }
        IndexColumn first = cols[0];
        if (first.sortType != SortOrder.ASCENDING) {
            return -1;
        }
        switch(first.column.getType()) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
            break;
        default:
            return -1;
        }
        return first.column.getColumnId();
    }

    public boolean canGetRowCount() {
        return true;
    }

    private void addRowsToIndex(Session session, ArrayList<Row> list, Index index) {
        final Index idx = index;
        Collections.sort(list, new Comparator<Row>() {
            public int compare(Row r1, Row r2) {
                return idx.compareRows(r1, r2);
            }
        });
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    public boolean canDrop() {
        return true;
    }

    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            return getScanIndex(session).getRowCount(session);
        }
        return rowCount;
    }

    public void removeRow(Session session, Row row) {
        if (database.isMultiVersion()) {
            if (row.isDeleted()) {
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, getName());
            }
            int old = row.getSessionId();
            int newId = session.getId();
            if (old == 0) {
                row.setSessionId(newId);
            } else if (old != newId) {
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, getName());
            }
        }
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
                trace.error("Could not undo operation", e);
                throw e2;
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    public void truncate(Session session) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        rowCount = 0;
        changesSinceAnalyze = 0;
    }

    private void analyzeIfRequired(Session session) {
        if (nextAnalyze == 0 || nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        changesSinceAnalyze = 0;
        int n = 2 * nextAnalyze;
        if (n > 0) {
            nextAnalyze = n;
        }
        int rows = SysProperties.ANALYZE_SAMPLE;
        Analyze.analyzeTable(session, this, rows, false);
    }

    public boolean isLockedExclusivelyBy(Session session) {
        return lockExclusive == session;
    }

    public void lock(Session session, boolean exclusive, boolean force) {
        int lockMode = database.getLockMode();
        if (lockMode == Constants.LOCK_MODE_OFF) {
            return;
        }
        if (!force && database.isMultiVersion()) {
            // MVCC: update, delete, and insert use a shared lock.
            // Select doesn't lock except when using FOR UPDATE and
            // the system property h2.selectForUpdateMvcc
            // is not enabled
            if (exclusive) {
                exclusive = false;
            } else {
                if (lockExclusive == null) {
                    return;
                }
            }
        }
        if (lockExclusive == session) {
            return;
        }
        synchronized (database) {
            try {
                doLock(session, lockMode, exclusive);
            } finally {
                session.setWaitForLock(null);
            }
        }
    }

    private void doLock(Session session, int lockMode, boolean exclusive) {
        traceLock(session, exclusive, "requesting for");
        long max = System.currentTimeMillis() + session.getLockTimeout();
        boolean checkDeadlock = false;
        while (true) {
            if (lockExclusive == session) {
                return;
            }
            if (exclusive) {
                if (lockExclusive == null) {
                    if (lockShared.isEmpty()) {
                        traceLock(session, exclusive, "added for");
                        session.addLock(this);
                        lockExclusive = session;
                        return;
                    } else if (lockShared.size() == 1 && lockShared.contains(session)) {
                        traceLock(session, exclusive, "add (upgraded) for ");
                        lockExclusive = session;
                        return;
                    }
                }
            } else {
                if (lockExclusive == null) {
                    if (lockMode == Constants.LOCK_MODE_READ_COMMITTED) {
                        if (!database.isMultiThreaded() && !database.isMultiVersion()) {
                            // READ_COMMITTED: a read lock is acquired,
                            // but released immediately after the operation
                            // is complete.
                            // When allowing only one thread, no lock is
                            // required.
                            // Row level locks work like read committed.
                            return;
                        }
                    }
                    if (!lockShared.contains(session)) {
                        traceLock(session, exclusive, "ok");
                        session.addLock(this);
                        lockShared.add(session);
                    }
                    return;
                }
            }
            session.setWaitForLock(this);
            if (checkDeadlock) {
                ArrayList<Session> sessions = checkDeadlock(session, null, null);
                if (sessions != null) {
                    throw DbException.get(ErrorCode.DEADLOCK_1, getDeadlockDetails(sessions));
                }
            } else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }
            long now = System.currentTimeMillis();
            if (now >= max) {
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
                long sleep = Math.min(Constants.DEADLOCK_CHECK, max - now);
                if (sleep == 0) {
                    sleep = 1;
                }
                waitForLock = true;
                database.wait(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private String getDeadlockDetails(ArrayList<Session> sessions) {
        StringBuilder buff = new StringBuilder();
        for (Session s : sessions) {
            Table lock = s.getWaitForLock();
            buff.append("\nSession ").
                append(s.toString()).
                append(" is waiting to lock ").
                append(lock.toString()).
                append(" while locking ");
            int i = 0;
            for (Table t : s.getLocks()) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(t.toString());
                if (t instanceof RegularTable) {
                    if (((RegularTable) t).lockExclusive == s) {
                        buff.append(" (exclusive)");
                    } else {
                        buff.append(" (shared)");
                    }
                }
            }
            buff.append('.');
        }
        return buff.toString();
    }

    public ArrayList<Session> checkDeadlock(Session session, Session clash, Set<Session> visited) {
        // only one deadlock check at any given time
        synchronized (RegularTable.class) {
            if (clash == null) {
                // verification is started
                clash = session;
                visited = New.hashSet();
            } else if (clash == session) {
                // we found a circle where this session is involved
                return New.arrayList();
            } else if (visited.contains(session)) {
                // we have already checked this session.
                // there is a circle, but the sessions in the circle need to
                // find it out themselves
                return null;
            }
            visited.add(session);
            ArrayList<Session> error = null;
            for (Session s : lockShared) {
                if (s == session) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }
                Table t = s.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(s, clash, visited);
                    if (error != null) {
                        error.add(session);
                        break;
                    }
                }
            }
            if (error == null && lockExclusive != null) {
                Table t = lockExclusive.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(lockExclusive, clash, visited);
                    if (error != null) {
                        error.add(session);
                    }
                }
            }
            return error;
        }
    }

    private void traceLock(Session session, boolean exclusive, String s) {
        if (traceLock.isDebugEnabled()) {
            traceLock.debug(session.getId() + " " + (exclusive ? "exclusive write lock" : "shared read lock") + " " + s + " " + getName());
        }
    }

    public boolean isLockedExclusively() {
        return lockExclusive != null;
    }

    public void unlock(Session s) {
        if (database != null) {
            traceLock(s, lockExclusive == s, "unlock");
            if (lockExclusive == s) {
                lockExclusive = null;
            }
            if (lockShared.size() > 0) {
                lockShared.remove(s);
            }
            // TODO lock: maybe we need we fifo-queue to make sure nobody
            // starves. check what other databases do
            synchronized (database) {
                if (database.getSessionCount() > 1 && waitForLock) {
                    database.notifyAll();
                }
            }
        }
    }

    /**
     * Create a row from the values.
     *
     * @param data the value list
     * @return the row
     */
    public Row createRow(Value[] data) {
        return new Row(data, memoryPerRow);
    }

    /**
     * Set the row count of this table.
     *
     * @param count the row count
     */
    public void setRowCount(long count) {
        this.rowCount = count;
    }

    public void removeChildrenAndResources(Session session) {
        super.removeChildrenAndResources(session);
        // go backwards because database.removeIndex will call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
        }
        if (SysProperties.CHECK) {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.INDEX)) {
                Index index = (Index) obj;
                if (index.getTable() == this) {
                    DbException.throwInternalError("index not dropped: " + index.getName());
                }
            }
        }
        scanIndex.remove(session);
        database.removeMeta(session, getId());
        scanIndex = null;
        lockExclusive = null;
        lockShared = null;
        invalidate();
    }

    public String toString() {
        return getSQL();
    }

    public void checkRename() {
        // ok
    }

    public void checkSupportAlter() {
        // ok
    }

    public boolean canTruncate() {
        ArrayList<Constraint> constraints = getConstraints();
        for (int i = 0; constraints != null && i < constraints.size(); i++) {
            Constraint c = constraints.get(i);
            if (!(c.getConstraintType().equals(Constraint.REFERENTIAL))) {
                continue;
            }
            ConstraintReferential ref = (ConstraintReferential) c;
            if (ref.getRefTable() == this) {
                return false;
            }
        }
        return true;
    }

    public String getTableType() {
        return Table.TABLE;
    }

    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
    }

    public long getRowCountApproximation() {
        return scanIndex.getRowCountApproximation();
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    public boolean isDeterministic() {
        return true;
    }

}
