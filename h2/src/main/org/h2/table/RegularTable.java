/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;

/**
 * Most tables are an instance of this class. For this table, the data is stored
 * in the database. The actual data is not kept here, instead it is kept in the
 * indexes. There is at least one index, the scan index.
 */
public abstract class RegularTable extends TableBase {

    /**
     * Appends the specified rows to the specified index.
     *
     * @param session
     *            the session
     * @param list
     *            the rows, list is cleared on completion
     * @param index
     *            the index to append to
     */
    protected static void addRowsToIndex(SessionLocal session, ArrayList<Row> list, Index index) {
        sortRows(list, index);
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    /**
     * Formats details of a deadlock.
     *
     * @param sessions
     *            the list of sessions
     * @param exclusive
     *            true if waiting for exclusive lock, false otherwise
     * @return formatted details of a deadlock
     */
    protected static String getDeadlockDetails(ArrayList<SessionLocal> sessions, boolean exclusive) {
        // We add the thread details here to make it easier for customers to
        // match up these error messages with their own logs.
        StringBuilder builder = new StringBuilder();
        for (SessionLocal s : sessions) {
            Table lock = s.getWaitForLock();
            Thread thread = s.getWaitForLockThread();
            builder.append("\nSession ").append(s.toString()).append(" on thread ").append(thread.getName())
                    .append(" is waiting to lock ").append(lock.toString())
                    .append(exclusive ? " (exclusive)" : " (shared)").append(" while locking ");
            boolean addComma = false;
            for (Table t : s.getLocks()) {
                if (addComma) {
                    builder.append(", ");
                }
                addComma = true;
                builder.append(t.toString());
                if (t instanceof RegularTable) {
                    if (((RegularTable) t).lockExclusiveSession == s) {
                        builder.append(" (exclusive)");
                    } else {
                        builder.append(" (shared)");
                    }
                }
            }
            builder.append('.');
        }
        return builder.toString();
    }

    /**
     * Sorts the specified list of rows for a specified index.
     *
     * @param list
     *            the list of rows
     * @param index
     *            the index to sort for
     */
    protected static void sortRows(ArrayList<? extends SearchRow> list, final Index index) {
        list.sort(index::compareRows);
    }

    /**
     * Whether the table contains a CLOB or BLOB.
     */
    protected final boolean containsLargeObject;

    /**
     * The session (if any) that has exclusively locked this table.
     */
    protected volatile SessionLocal lockExclusiveSession;

    /**
     * The set of sessions (if any) that have a shared lock on the table. Here
     * we are using using a ConcurrentHashMap as a set, as there is no
     * ConcurrentHashSet.
     */
    protected final ConcurrentHashMap<SessionLocal, SessionLocal> lockSharedSessions = new ConcurrentHashMap<>();

    private Column rowIdColumn;

    protected RegularTable(CreateTableData data) {
        super(data);
        this.isHidden = data.isHidden;
        boolean b = false;
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType().getValueType())) {
                b = true;
                break;
            }
        }
        containsLargeObject = b;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return true;
    }

    @Override
    public boolean canTruncate() {
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (Constraint c : constraints) {
                    if (c.getConstraintType() != Constraint.Type.REFERENTIAL) {
                        continue;
                    }
                    ConstraintReferential ref = (ConstraintReferential) c;
                    if (ref.getRefTable() == this) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public ArrayList<SessionLocal> checkDeadlock(SessionLocal session, SessionLocal clash, Set<SessionLocal> visited) {
        // only one deadlock check at any given time
        synchronized (getClass()) {
            if (clash == null) {
                // verification is started
                clash = session;
                visited = new HashSet<>();
            } else if (clash == session) {
                // we found a cycle where this session is involved
                return new ArrayList<>(0);
            } else if (visited.contains(session)) {
                // we have already checked this session.
                // there is a cycle, but the sessions in the cycle need to
                // find it out themselves
                return null;
            }
            visited.add(session);
            ArrayList<SessionLocal> error = null;
            for (SessionLocal s : lockSharedSessions.keySet()) {
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
            // take a local copy so we don't see inconsistent data, since we are
            // not locked while checking the lockExclusiveSession value
            SessionLocal copyOfLockExclusiveSession = lockExclusiveSession;
            if (error == null && copyOfLockExclusiveSession != null) {
                Table t = copyOfLockExclusiveSession.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(copyOfLockExclusiveSession, clash, visited);
                    if (error != null) {
                        error.add(session);
                    }
                }
            }
            return error;
        }
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, TypeInfo.TYPE_BIGINT, this, SearchRow.ROWID_INDEX);
            rowIdColumn.setRowId(true);
            rowIdColumn.setNullable(false);
        }
        return rowIdColumn;
    }

    @Override
    public TableType getTableType() {
        return TableType.TABLE;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean isLockedExclusively() {
        return lockExclusiveSession != null;
    }

    @Override
    public boolean isLockedExclusivelyBy(SessionLocal session) {
        return lockExclusiveSession == session;
    }

    @Override
    protected void invalidate() {
        super.invalidate();
        /*
         * Query cache of a some sleeping session can have references to
         * invalidated tables. When this table was dropped by another session,
         * the field below still points to it and prevents its garbage
         * collection, so this field needs to be cleared to prevent a memory
         * leak.
         */
        lockExclusiveSession = null;
    }

    @Override
    public String toString() {
        return getTraceSQL();
    }

    /**
     * Prepares columns of an index.
     *
     * @param database the database
     * @param cols the index columns
     * @param indexType the type of an index
     * @return the prepared columns with flags set
     */
    protected static IndexColumn[] prepareColumns(Database database, IndexColumn[] cols, IndexType indexType) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
            }
            for (IndexColumn c : cols) {
                c.column.setPrimaryKey(true);
            }
        } else if (!indexType.isSpatial()) {
            int i = 0, l = cols.length;
            while (i < l && (cols[i].sortType & (SortOrder.NULLS_FIRST | SortOrder.NULLS_LAST)) != 0) {
                i++;
            }
            if (i != l) {
                cols = cols.clone();
                DefaultNullOrdering defaultNullOrdering = database.getDefaultNullOrdering();
                for (; i < l; i++) {
                    IndexColumn oldColumn = cols[i];
                    int sortTypeOld = oldColumn.sortType;
                    int sortTypeNew = defaultNullOrdering.addExplicitNullOrdering(sortTypeOld);
                    if (sortTypeNew != sortTypeOld) {
                        IndexColumn newColumn = new IndexColumn(oldColumn.columnName, sortTypeNew);
                        newColumn.column = oldColumn.column;
                        cols[i] = newColumn;
                    }
                }
            }
        }
        return cols;
    }

}
