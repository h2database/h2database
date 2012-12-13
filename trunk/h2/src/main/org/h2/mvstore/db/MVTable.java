/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.h2.api.DatabaseEventListener;
import org.h2.command.ddl.CreateTableData;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MultiVersionIndex;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.Value;

/**
 * A table stored in a MVStore.
 */
public class MVTable extends TableBase {

    private final String storeName;
    private final MVStore store;
    private final boolean hidden;
    private MVPrimaryIndex primaryIndex;
    private ArrayList<Index> indexes = New.arrayList();
    private long lastModificationId;
    private long rowCount;

    public MVTable(CreateTableData data, String storeName, MVStore store) {
        super(data);
        this.storeName = storeName;
        this.store = store;
        this.hidden = data.isHidden;
    }

    void init(Session session) {
        primaryIndex = new MVPrimaryIndex(session.getDatabase(),
                this, getId(),
                IndexColumn.wrap(getColumns()),
                IndexType.createScan(true)
                );
        rowCount = primaryIndex.getRowCount(session);
        indexes.add(primaryIndex);
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {
        // TODO locking
    }

    @Override
    public boolean isHidden() {
        return hidden;
    }

    @Override
    public boolean canTruncate() {
        // TODO copy & pasted source code from RegularTable
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (int i = 0, size = constraints.size(); i < size; i++) {
                    Constraint c = constraints.get(i);
                    if (!(c.getConstraintType().equals(Constraint.REFERENTIAL))) {
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
    public void unlock(Session s) {
        // TODO locking
    }

    @Override
    public boolean isLockedExclusively() {
        // TODO locking
        return false;
    }

    @Override
    public void close(Session session) {
        MVTableEngine.closeTable(storeName, this);
    }

    Row getRow(Session session, long key) {
        return primaryIndex.getRow(session, key);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
            IndexColumn[] cols, IndexType indexType, boolean create,
            String indexComment) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        Index index;
        // TODO support in-memory indexes
      //  if (isPersistIndexes() && indexType.isPersistent()) {
            int mainIndexColumn;
            mainIndexColumn = getMainIndexColumn(indexType, cols);
//            if (database.isStarting()) {
//                mainIndexColumn = -1;
//            } else if (!database.isStarting() && primaryIndex.getRowCount(session) != 0) {
//                mainIndexColumn = -1;
//            } else {
//            }
            if (mainIndexColumn != -1) {
                primaryIndex.setMainIndexColumn(mainIndexColumn);
                index = new MVDelegateIndex(this, indexId,
                        indexName, primaryIndex, indexType);
            } else {
                index = new MVSecondaryIndex(session.getDatabase(),
                        this, indexId,
                        indexName, cols, indexType);
            }
//        } else {
//            index = new TreeIndex(this, indexId, indexName, cols, indexType);
//        }
        if (index.needRebuild() && rowCount > 0) {
            try {
                Index scan = getScanIndex(session);
                long remaining = scan.getRowCount(session);
                long total = remaining;
                Cursor cursor = scan.find(session, null, null);
                long i = 0;
                int bufferSize = (int) Math.min(rowCount, Constants.DEFAULT_MAX_MEMORY_ROWS);
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

    private int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (primaryIndex.getMainIndexColumn() != -1) {
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
        storeIfRequired();
    }

    @Override
    public void removeRow(Session session, Row row) {
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
        storeIfRequired();
    }

    @Override
    public void truncate(Session session) {
        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        rowCount = 0;
        storeIfRequired();
    }

    @Override
    public void addRow(Session session, Row row) {
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
            DbException de = DbException.convert(e);
            if (de.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                for (int j = 0; j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (index.getIndexType().isUnique() && index instanceof MultiVersionIndex) {
                        MultiVersionIndex mv = (MultiVersionIndex) index;
                        if (mv.isUncommittedFromOtherSession(session, row)) {
                            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, index.getName());
                        }
                    }
                }
            }
            throw de;
        }
        analyzeIfRequired(session);
        storeIfRequired();
    }

    private void checkRowCount(Session session, Index index, int offset) {
        // TODO verify
    }

    private void analyzeIfRequired(Session session) {
        // TODO analyze
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    @Override
    public String getTableType() {
        return Table.EXTERNAL_TABLE_ENGINE;
    }

    @Override
    public Index getScanIndex(Session session) {
        return primaryIndex;
    }

    @Override
    public Index getUniqueIndex() {
        return primaryIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public boolean canDrop() {
        return true;
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
        primaryIndex.remove(session);
        database.removeMeta(session, getId());
        primaryIndex = null;
        close(session);
        invalidate();
    }

    @Override
    public long getRowCount(Session session) {
        return primaryIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return primaryIndex.getRowCountApproximation();
    }

    public long getDiskSpaceUsed() {
        return primaryIndex.getDiskSpaceUsed();
    }

    @Override
    public void checkRename() {
        // ok
    }

    private void storeIfRequired() {
        if (store.getUnsavedPageCount() > 1000) {
            store.store();
        }
    }

    public MVStore getStore() {
        return store;
    }

}
