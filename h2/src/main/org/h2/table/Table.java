/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.constraint.Constraint;
import org.h2.constraint.Constraint.Type;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.DefaultRow;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SimpleRowValue;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This is the base class for most tables.
 * A table contains a list of columns and a list of rows.
 */
public abstract class Table extends SchemaObject {

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_CACHED = 0;

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_MEMORY = 1;

    /**
     * Read lock.
     */
    public static final int READ_LOCK = 0;

    /**
     * Write lock.
     */
    public static final int WRITE_LOCK = 1;

    /**
     * Exclusive lock.
     */
    public static final int EXCLUSIVE_LOCK = 2;

    /**
     * The columns of this table.
     */
    protected Column[] columns;

    /**
     * The compare mode used for this table.
     */
    protected CompareMode compareMode;

    /**
     * Protected tables are not listed in the meta data and are excluded when
     * using the SCRIPT command.
     */
    protected boolean isHidden;

    private final HashMap<String, Column> columnMap;
    private final boolean persistIndexes;
    private final boolean persistData;
    private ArrayList<TriggerObject> triggers;
    private ArrayList<Constraint> constraints;
    private ArrayList<Sequence> sequences;
    /**
     * views that depend on this table
     */
    private final CopyOnWriteArrayList<TableView> dependentViews = new CopyOnWriteArrayList<>();
    /**
     * materialized views that depend on this table
     */
    private final CopyOnWriteArrayList<MaterializedView> dependentMaterializedViews = new CopyOnWriteArrayList<>();
    private ArrayList<TableSynonym> synonyms;
    /** Is foreign key constraint checking enabled for this table. */
    private boolean checkForeignKeyConstraints = true;
    private boolean onCommitDrop, onCommitTruncate;
    private volatile Row nullRow;
    private RowFactory rowFactory = RowFactory.getRowFactory();
    private boolean tableExpression;

    protected Table(Schema schema, int id, String name, boolean persistIndexes, boolean persistData) {
        super(schema, id, name, Trace.TABLE);
        columnMap = schema.getDatabase().newStringMap();
        this.persistIndexes = persistIndexes;
        this.persistData = persistData;
        compareMode = schema.getDatabase().getCompareMode();
    }

    @Override
    public void rename(String newName) {
        super.rename(newName);
        if (constraints != null) {
            for (Constraint constraint : constraints) {
                constraint.rebuild();
            }
        }
    }

    public boolean isView() {
        return false;
    }

    /**
     * Lock the table for the given session.
     * This method waits until the lock is granted.
     *
     * @param session the session
     * @param lockType the type of lock
     * @return true if the table was already exclusively locked by this session.
     * @throws DbException if a lock timeout occurred
     */
    public boolean lock(SessionLocal session, int lockType) {
        return false;
    }

    /**
     * Close the table object and flush changes.
     *
     * @param session the session
     */
    public abstract void close(SessionLocal session);

    /**
     * Release the lock for this session.
     *
     * @param s the session
     */
    public void unlock(SessionLocal s) {
    }

    /**
     * Create an index for this table
     *
     * @param session the session
     * @param indexName the name of the index
     * @param indexId the id
     * @param cols the index columns
     * @param uniqueColumnCount the count of unique columns
     * @param indexType the index type
     * @param create whether this is a new index
     * @param indexComment the comment
     * @return the index
     */
    public abstract Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment);

    /**
     * Get the given row.
     *
     * @param session the session
     * @param key the primary key
     * @return the row
     */
    @SuppressWarnings("unused")
    public Row getRow(SessionLocal session, long key) {
        return null;
    }

    /**
     * Returns whether this table is insertable.
     *
     * @return whether this table is insertable
     */
    public boolean isInsertable() {
        return true;
    }

    /**
     * Remove a row from the table and all indexes.
     *
     * @param session the session
     * @param row the row
     */
    public abstract void removeRow(SessionLocal session, Row row);

    /**
     * Locks row, preventing any updated to it, except from the session specified.
     *
     * @param session the session
     * @param row to lock
     * @param timeoutMillis
     *            timeout in milliseconds, {@code -1} for default, {@code -2} to
     *            skip locking if row is already locked by another session
     * @return locked row, or null if row does not exist anymore or if it was skipped
     */
    public Row lockRow(SessionLocal session, Row row, int timeoutMillis) {
        throw DbException.getUnsupportedException("lockRow()");
    }

    /**
     * Remove all rows from the table and indexes.
     *
     * @param session the session
     * @return number of removed rows, possibly including uncommitted rows
     */
    public abstract long truncate(SessionLocal session);

    /**
     * Add a row to the table and all indexes.
     *
     * @param session the session
     * @param row the row
     * @throws DbException if a constraint was violated
     */
    public abstract void addRow(SessionLocal session, Row row);

    /**
     * Update a row to the table and all indexes.
     *
     * @param session the session
     * @param oldRow the row to update
     * @param newRow the row with updated values (_rowid_ suppose to be the same)
     * @throws DbException if a constraint was violated
     */
    public void updateRow(SessionLocal session, Row oldRow, Row newRow) {
        newRow.setKey(oldRow.getKey());
        removeRow(session, oldRow);
        addRow(session, newRow);
    }

    /**
     * Check if this table supports ALTER TABLE.
     *
     * @throws DbException if it is not supported
     */
    public abstract void checkSupportAlter();

    /**
     * Get the table type name
     *
     * @return the table type name
     */
    public abstract TableType getTableType();

    /**
     * Return SQL table type for INFORMATION_SCHEMA.
     *
     * @return SQL table type for INFORMATION_SCHEMA
     */
    public String getSQLTableType() {
        if (isView()) {
            return "VIEW";
        }
        if (isTemporary()) {
            return isGlobalTemporary() ? "GLOBAL TEMPORARY" : "LOCAL TEMPORARY";
        }
        return "BASE TABLE";
    }

    /**
     * Get the scan index to iterate through all rows.
     *
     * @param session the session
     * @return the index
     */
    public abstract Index getScanIndex(SessionLocal session);

    /**
     * Get the scan index for this table.
     *
     * @param session the session
     * @param masks the search mask
     * @param filters the table filters
     * @param filter the filter index
     * @param sortOrder the sort order
     * @param allColumnsSet all columns
     * @return the scan index
     */
    @SuppressWarnings("unused")
    public Index getScanIndex(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return getScanIndex(session);
    }

    /**
     * Get all indexes for this table.
     *
     * @return the list of indexes
     */
    public abstract ArrayList<Index> getIndexes();

    /**
     * Get an index by name.
     *
     * @param indexName the index name to search for
     * @return the found index
     */
    public Index getIndex(String indexName) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getName().equals(indexName)) {
                    return index;
                }
            }
        }
        throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);
    }

    /**
     * Check if this table is locked exclusively.
     *
     * @return true if it is.
     */
    public boolean isLockedExclusively() {
        return false;
    }

    /**
     * Get the last data modification id.
     *
     * @return the modification id
     */
    public abstract long getMaxDataModificationId();

    /**
     * Check if the table is deterministic.
     *
     * @return true if it is
     */
    public abstract boolean isDeterministic();

    /**
     * Check if the row count can be retrieved quickly.
     *
     * @param session the session
     * @return true if it can
     */
    public abstract boolean canGetRowCount(SessionLocal session);

    /**
     * Check if this table can be referenced.
     *
     * @return true if it can
     */
    public boolean canReference() {
        return true;
    }

    /**
     * Check if this table can be dropped.
     *
     * @return true if it can
     */
    public abstract boolean canDrop();

    /**
     * Get the row count for this table.
     *
     * @param session the session
     * @return the row count
     */
    public abstract long getRowCount(SessionLocal session);

    /**
     * Get the approximated row count for this table.
     *
     * @param session the session
     * @return the approximated row count
     */
    public abstract long getRowCountApproximation(SessionLocal session);

    public long getDiskSpaceUsed() {
        return 0L;
    }

    /**
     * Get the row id column if this table has one.
     *
     * @return the row id column, or null
     */
    public Column getRowIdColumn() {
        return null;
    }

    /**
     * Check whether the table (or view) contains no columns that prevent index
     * conditions to be used. For example, a view that contains the ROWNUM()
     * pseudo-column prevents this.
     *
     * @return true if the table contains no query-comparable column
     */
    public boolean isQueryComparable() {
        return true;
    }

    /**
     * Add all objects that this table depends on to the hash set.
     *
     * @param dependencies the current set of dependencies
     */
    public void addDependencies(HashSet<DbObject> dependencies) {
        if (dependencies.contains(this)) {
            // avoid endless recursion
            return;
        }
        if (sequences != null) {
            dependencies.addAll(sequences);
        }
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(
                dependencies);
        for (Column col : columns) {
            col.isEverything(visitor);
        }
        if (constraints != null) {
            for (Constraint c : constraints) {
                c.isEverything(visitor);
            }
        }
        dependencies.add(this);
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = Utils.newSmallArrayList();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            children.addAll(indexes);
        }
        if (constraints != null) {
            children.addAll(constraints);
        }
        if (triggers != null) {
            children.addAll(triggers);
        }
        if (sequences != null) {
            children.addAll(sequences);
        }
        children.addAll(dependentViews);
        if (synonyms != null) {
            children.addAll(synonyms);
        }
        ArrayList<Right> rights = database.getAllRights();
        for (Right right : rights) {
            if (right.getGrantedObject() == this) {
                children.add(right);
            }
        }
        return children;
    }

    protected void setColumns(Column[] columns) {
        if (columns.length > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }
        this.columns = columns;
        if (columnMap.size() > 0) {
            columnMap.clear();
        }
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            int dataType = col.getType().getValueType();
            if (dataType == Value.UNKNOWN) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, col.getTraceSQL());
            }
            col.setTable(this, i);
            String columnName = col.getName();
            if (columnMap.putIfAbsent(columnName, col) != null) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, columnName);
            }
        }
        rowFactory = database.getRowFactory().createRowFactory(database, database.getCompareMode(), database, columns,
                null, false);
    }

    /**
     * Rename a column of this table.
     *
     * @param column the column to rename
     * @param newName the new column name
     */
    public void renameColumn(Column column, String newName) {
        for (Column c : columns) {
            if (c == column) {
                continue;
            }
            if (c.getName().equals(newName)) {
                throw DbException.get(
                        ErrorCode.DUPLICATE_COLUMN_NAME_1, newName);
            }
        }
        columnMap.remove(column.getName());
        column.rename(newName);
        columnMap.put(newName, column);
    }

    /**
     * Check if the table is exclusively locked by this session.
     *
     * @param session the session
     * @return true if it is
     */
    @SuppressWarnings("unused")
    public boolean isLockedExclusivelyBy(SessionLocal session) {
        return false;
    }

    /**
     * Update a list of rows in this table.
     *
     * @param prepared the prepared statement
     * @param session the session
     * @param rows a list of row pairs of the form old row, new row, old row,
     *            new row,...
     */
    public void updateRows(Prepared prepared, SessionLocal session, LocalResult rows) {
        // in case we need to undo the update
        SessionLocal.Savepoint rollback = session.setSavepoint();
        // remove the old rows
        int rowScanCount = 0;
        while (rows.next()) {
            if ((++rowScanCount & 127) == 0) {
                prepared.checkCanceled();
            }
            Row o = rows.currentRowForTable();
            rows.next();
            try {
                removeRow(session, o);
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1
                        || e.getErrorCode() == ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1) {
                    session.rollbackTo(rollback);
                }
                throw e;
            }
        }
        // add the new rows
        rows.reset();
        while (rows.next()) {
            if ((++rowScanCount & 127) == 0) {
                prepared.checkCanceled();
            }
            rows.next();
            Row n = rows.currentRowForTable();
            try {
                addRow(session, n);
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1) {
                    session.rollbackTo(rollback);
                }
                throw e;
            }
        }
    }

    public CopyOnWriteArrayList<TableView> getDependentViews() {
        return dependentViews;
    }

    public CopyOnWriteArrayList<MaterializedView> getDependentMaterializedViews() {
        return dependentMaterializedViews;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        while (!dependentViews.isEmpty()) {
            TableView view = dependentViews.remove(0);
            database.removeSchemaObject(session, view);
        }
        while (synonyms != null && !synonyms.isEmpty()) {
            TableSynonym synonym = synonyms.remove(0);
            database.removeSchemaObject(session, synonym);
        }
        while (triggers != null && !triggers.isEmpty()) {
            TriggerObject trigger = triggers.remove(0);
            database.removeSchemaObject(session, trigger);
        }
        while (constraints != null && !constraints.isEmpty()) {
            Constraint constraint = constraints.remove(0);
            database.removeSchemaObject(session, constraint);
        }
        for (Right right : database.getAllRights()) {
            if (right.getGrantedObject() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        // must delete sequences later (in case there is a power failure
        // before removing the table object)
        while (sequences != null && !sequences.isEmpty()) {
            Sequence sequence = sequences.remove(0);
            // only remove if no other table depends on this sequence
            // this is possible when calling ALTER TABLE ALTER COLUMN
            if (database.getDependentTable(sequence, this) == null) {
                database.removeSchemaObject(session, sequence);
            }
        }
    }

    /**
     * Check that these columns are not referenced by a multi-column constraint
     * or multi-column index. If it is, an exception is thrown. Single-column
     * references and indexes are dropped.
     *
     * @param session the session
     * @param columnsToDrop the columns to drop
     * @throws DbException if the column is referenced by multi-column
     *             constraints or indexes
     */
    public void dropMultipleColumnsConstraintsAndIndexes(SessionLocal session,
            ArrayList<Column> columnsToDrop) {
        HashSet<Constraint> constraintsToDrop = new HashSet<>();
        if (constraints != null) {
            for (Column col : columnsToDrop) {
                for (Constraint constraint : constraints) {
                    HashSet<Column> columns = constraint.getReferencedColumns(this);
                    if (!columns.contains(col)) {
                        continue;
                    }
                    if (columns.size() == 1) {
                        constraintsToDrop.add(constraint);
                    } else {
                        throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, constraint.getTraceSQL());
                    }
                }
            }
        }
        HashSet<Index> indexesToDrop = new HashSet<>();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (Column col : columnsToDrop) {
                for (Index index : indexes) {
                    if (index.getCreateSQL() == null) {
                        continue;
                    }
                    if (index.getColumnIndex(col) < 0) {
                        continue;
                    }
                    if (index.getColumns().length == 1) {
                        indexesToDrop.add(index);
                    } else {
                        throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, index.getTraceSQL());
                    }
                }
            }
        }
        for (Constraint c : constraintsToDrop) {
            if (c.isValid()) {
                session.getDatabase().removeSchemaObject(session, c);
            }
        }
        for (Index i : indexesToDrop) {
            // the index may already have been dropped when dropping the
            // constraint
            if (getIndexes().contains(i)) {
                session.getDatabase().removeSchemaObject(session, i);
            }
        }
    }

    public RowFactory getRowFactory() {
        return rowFactory;
    }

    /**
     * Create a new row for this table.
     *
     * @param data the values
     * @param memory the estimated memory usage in bytes
     * @return the created row
     */
    public Row createRow(Value[] data, int memory) {
        return rowFactory.createRow(data, memory);
    }

    public Row getTemplateRow() {
        return createRow(new Value[getColumns().length], DefaultRow.MEMORY_CALCULATE);
    }

    /**
     * Get a new simple row object.
     *
     * @param singleColumn if only one value need to be stored
     * @return the simple row object
     */
    public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        if (singleColumn) {
            return new SimpleRowValue(columns.length);
        }
        return new DefaultRow(new Value[columns.length]);
    }

    public Row getNullRow() {
        Row row = nullRow;
        if (row == null) {
            // Here can be concurrently produced more than one row, but it must
            // be ok.
            Value[] values = new Value[columns.length];
            Arrays.fill(values, ValueNull.INSTANCE);
            nullRow = row = createRow(values, 1);
        }
        return row;
    }

    public Column[] getColumns() {
        return columns;
    }

    @Override
    public int getType() {
        return DbObject.TABLE_OR_VIEW;
    }

    /**
     * Get the column at the given index.
     *
     * @param index the column index (0, 1,...)
     * @return the column
     */
    public Column getColumn(int index) {
        return columns[index];
    }

    /**
     * Get the column with the given name.
     *
     * @param columnName the column name
     * @return the column
     * @throws DbException if the column was not found
     */
    public Column getColumn(String columnName) {
        Column column = columnMap.get(columnName);
        if (column == null) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    /**
     * Get the column with the given name.
     *
     * @param columnName the column name
     * @param ifExists if {@code true} return {@code null} if column does not exist
     * @return the column
     * @throws DbException if the column was not found
     */
    public Column getColumn(String columnName, boolean ifExists) {
        Column column = columnMap.get(columnName);
        if (column == null && !ifExists) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    /**
     * Get the column with the given name if it exists.
     *
     * @param columnName the column name, or {@code null}
     * @return the column
     */
    public Column findColumn(String columnName) {
        return columnMap.get(columnName);
    }

    /**
     * Does the column with the given name exist?
     *
     * @param columnName the column name
     * @return true if the column exists
     */
    public boolean doesColumnExist(String columnName) {
        return columnMap.containsKey(columnName);
    }

    /**
     * Returns first identity column, or {@code null}.
     *
     * @return first identity column, or {@code null}
     */
    public Column getIdentityColumn() {
        for (Column column : columns) {
            if (column.isIdentity()) {
                return column;
            }
        }
        return null;
    }

    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param allColumnsSet the set of all columns
     * @return the plan item
     */
    public PlanItem getBestPlanItem(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        PlanItem item = new PlanItem();
        item.setIndex(getScanIndex(session));
        item.cost = item.getIndex().getCost(session, null, filters, filter, null, allColumnsSet);
        Trace t = session.getTrace();
        if (t.isDebugEnabled()) {
            t.debug("Table      :     potential plan item cost {0} index {1}",
                    item.cost, item.getIndex().getPlanSQL());
        }
        ArrayList<Index> indexes = getIndexes();
        IndexHints indexHints = getIndexHints(filters, filter);

        if (indexes != null && masks != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);

                if (isIndexExcludedByHints(indexHints, index)) {
                    continue;
                }

                double cost = index.getCost(session, masks, filters, filter,
                        sortOrder, allColumnsSet);
                if (t.isDebugEnabled()) {
                    t.debug("Table      :     potential plan item cost {0} index {1}",
                            cost, index.getPlanSQL());
                }
                if (cost < item.cost) {
                    item.cost = cost;
                    item.setIndex(index);
                }
            }
        }
        return item;
    }

    private static boolean isIndexExcludedByHints(IndexHints indexHints, Index index) {
        return indexHints != null && !indexHints.allowIndex(index);
    }

    private static IndexHints getIndexHints(TableFilter[] filters, int filter) {
        return filters == null ? null : filters[filter].getIndexHints();
    }

    /**
     * Get the primary key index if there is one, or null if there is none.
     *
     * @return the primary key index or null
     */
    public Index findPrimaryKey() {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (Index idx : indexes) {
                if (idx.getIndexType().isPrimaryKey()) {
                    return idx;
                }
            }
        }
        return null;
    }

    public Index getPrimaryKey() {
        Index index = findPrimaryKey();
        if (index != null) {
            return index;
        }
        throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1,
                Constants.PREFIX_PRIMARY_KEY);
    }

    /**
     * Prepares the specified row for INSERT operation.
     *
     * Identity, default, and generated values are evaluated, all values are
     * converted to target data types and validated. Base value of identity
     * column is updated when required by compatibility mode.
     *
     * @param session the session
     * @param overridingSystem
     *            {@link Boolean#TRUE} for {@code OVERRIDING SYSTEM VALUES},
     *            {@link Boolean#FALSE} for {@code OVERRIDING USER VALUES},
     *            {@code null} if override clause is not specified
     * @param row the row
     */
    public void convertInsertRow(SessionLocal session, Row row, Boolean overridingSystem) {
        int length = columns.length, generated = 0;
        for (int i = 0; i < length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            if (value == ValueNull.INSTANCE && column.isDefaultOnNull()) {
                value = null;
            }
            if (column.isIdentity()) {
                if (overridingSystem != null) {
                    if (!overridingSystem) {
                        value = null;
                    }
                } else if (value != null && column.isGeneratedAlways()) {
                    throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1,
                            column.getSQLWithTable(new StringBuilder(), TRACE_SQL_FLAGS).toString());
                }
            } else if (column.isGeneratedAlways()) {
                if (value != null) {
                    throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1,
                            column.getSQLWithTable(new StringBuilder(), TRACE_SQL_FLAGS).toString());
                }
                generated++;
                continue;
            }
            Value v2 = column.validateConvertUpdateSequence(session, value, row);
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
        if (generated > 0) {
            for (int i = 0; i < length; i++) {
                Value value = row.getValue(i);
                if (value == null) {
                    row.setValue(i, columns[i].validateConvertUpdateSequence(session, null, row));
                }
            }
        }
    }

    /**
     * Prepares the specified row for UPDATE operation.
     *
     * Default and generated values are evaluated, all values are converted to
     * target data types and validated. Base value of identity column is updated
     * when required by compatibility mode.
     *
     * @param session the session
     * @param row the row
     * @param fromTrigger {@code true} if row was modified by INSERT or UPDATE trigger
     */
    public void convertUpdateRow(SessionLocal session, Row row, boolean fromTrigger) {
        int length = columns.length, generated = 0;
        for (int i = 0; i < length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            if (column.isGenerated()) {
                if (value != null) {
                    if (!fromTrigger) {
                        throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1,
                                column.getSQLWithTable(new StringBuilder(), TRACE_SQL_FLAGS).toString());
                    }
                    row.setValue(i, null);
                }
                generated++;
                continue;
            }
            Value v2 = column.validateConvertUpdateSequence(session, value, row);
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
        if (generated > 0) {
            for (int i = 0; i < length; i++) {
                Value value = row.getValue(i);
                if (value == null) {
                    row.setValue(i, columns[i].validateConvertUpdateSequence(session, null, row));
                }
            }
        }
    }

    private static void remove(ArrayList<? extends DbObject> list, DbObject obj) {
        if (list != null) {
            list.remove(obj);
        }
    }

    /**
     * Remove the given index from the list.
     *
     * @param index the index to remove
     */
    public void removeIndex(Index index) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            remove(indexes, index);
            if (index.getIndexType().isPrimaryKey()) {
                for (Column col : index.getColumns()) {
                    col.setPrimaryKey(false);
                }
            }
        }
    }

    /**
     * Remove the given view from the dependent views list.
     *
     * @param view the view to remove
     */
    public void removeDependentView(TableView view) {
        dependentViews.remove(view);
    }

    /**
     * Remove the given view from the dependent views list.
     *
     * @param view the view to remove
     */
    public void removeDependentMaterializedView(MaterializedView view) {
        dependentMaterializedViews.remove(view);
    }

    /**
     * Remove the given view from the list.
     *
     * @param synonym the synonym to remove
     */
    public void removeSynonym(TableSynonym synonym) {
        remove(synonyms, synonym);
    }

    /**
     * Remove the given constraint from the list.
     *
     * @param constraint the constraint to remove
     */
    public void removeConstraint(Constraint constraint) {
        remove(constraints, constraint);
    }

    /**
     * Remove a sequence from the table. Sequences are used as identity columns.
     *
     * @param sequence the sequence to remove
     */
    public final void removeSequence(Sequence sequence) {
        remove(sequences, sequence);
    }

    /**
     * Remove the given trigger from the list.
     *
     * @param trigger the trigger to remove
     */
    public void removeTrigger(TriggerObject trigger) {
        remove(triggers, trigger);
    }

    /**
     * Add a view to this table.
     *
     * @param view the view to add
     */
    public void addDependentView(TableView view) {
        dependentViews.add(view);
    }

    /**
     * Add a materialized view to this table.
     *
     * @param view the view to add
     */
    public void addDependentMaterializedView(MaterializedView view) {
        this.dependentMaterializedViews.add(view);
    }

    /**
     * Add a synonym to this table.
     *
     * @param synonym the synonym to add
     */
    public void addSynonym(TableSynonym synonym) {
        synonyms = add(synonyms, synonym);
    }

    /**
     * Add a constraint to the table.
     *
     * @param constraint the constraint to add
     */
    public void addConstraint(Constraint constraint) {
        if (constraints == null || !constraints.contains(constraint)) {
            constraints = add(constraints, constraint);
        }
    }

    public ArrayList<Constraint> getConstraints() {
        return constraints;
    }

    /**
     * Add a sequence to this table.
     *
     * @param sequence the sequence to add
     */
    public void addSequence(Sequence sequence) {
        sequences = add(sequences, sequence);
    }

    /**
     * Add a trigger to this table.
     *
     * @param trigger the trigger to add
     */
    public void addTrigger(TriggerObject trigger) {
        triggers = add(triggers, trigger);
    }

    private static <T> ArrayList<T> add(ArrayList<T> list, T obj) {
        if (list == null) {
            list = Utils.newSmallArrayList();
        }
        // self constraints are two entries in the list
        list.add(obj);
        return list;
    }

    /**
     * Fire the triggers for this table.
     *
     * @param session the session
     * @param type the trigger type
     * @param beforeAction whether 'before' triggers should be called
     */
    public void fire(SessionLocal session, int type, boolean beforeAction) {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                trigger.fire(session, type, beforeAction);
            }
        }
    }

    /**
     * Check whether this table has a select trigger.
     *
     * @return true if it has
     */
    public boolean hasSelectTrigger() {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                if (trigger.isSelectTrigger()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if row based triggers or constraints are defined.
     * In this case the fire after and before row methods need to be called.
     *
     *  @return if there are any triggers or rows defined
     */
    public boolean fireRow() {
        return (constraints != null && !constraints.isEmpty()) ||
                (triggers != null && !triggers.isEmpty());
    }

    /**
     * Fire all triggers that need to be called before a row is updated.
     *
     * @param session the session
     * @param oldRow the old data or null for an insert
     * @param newRow the new data or null for a delete
     * @return true if no further action is required (for 'instead of' triggers)
     */
    public boolean fireBeforeRow(SessionLocal session, Row oldRow, Row newRow) {
        boolean done = fireRow(session, oldRow, newRow, true, false);
        fireConstraints(session, oldRow, newRow, true);
        return done;
    }

    private void fireConstraints(SessionLocal session, Row oldRow, Row newRow,
            boolean before) {
        if (constraints != null) {
            for (Constraint constraint : constraints) {
                if (constraint.isBefore() == before) {
                    constraint.checkRow(session, this, oldRow, newRow);
                }
            }
        }
    }

    /**
     * Fire all triggers that need to be called after a row is updated.
     *
     *  @param session the session
     *  @param oldRow the old data or null for an insert
     *  @param newRow the new data or null for a delete
     *  @param rollback when the operation occurred within a rollback
     */
    public void fireAfterRow(SessionLocal session, Row oldRow, Row newRow,
            boolean rollback) {
        fireRow(session, oldRow, newRow, false, rollback);
        if (!rollback) {
            fireConstraints(session, oldRow, newRow, false);
        }
    }

    private boolean fireRow(SessionLocal session, Row oldRow, Row newRow,
            boolean beforeAction, boolean rollback) {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                boolean done = trigger.fireRow(session, this, oldRow, newRow, beforeAction, rollback);
                if (done) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isGlobalTemporary() {
        return false;
    }

    /**
     * Check if this table can be truncated.
     *
     * @return true if it can
     */
    public boolean canTruncate() {
        return false;
    }

    /**
     * Enable or disable foreign key constraint checking for this table.
     *
     * @param session the session
     * @param enabled true if checking should be enabled
     * @param checkExisting true if existing rows must be checked during this
     *            call
     */
    public void setCheckForeignKeyConstraints(SessionLocal session, boolean enabled, boolean checkExisting) {
        if (enabled && checkExisting) {
            if (constraints != null) {
                for (Constraint c : constraints) {
                    if (c.getConstraintType() == Type.REFERENTIAL) {
                        c.checkExistingData(session);
                    }
                }
            }
        }
        checkForeignKeyConstraints = enabled;
    }

    /**
     * @return is foreign key constraint checking enabled for this table.
     */
    public boolean getCheckForeignKeyConstraints() {
        return checkForeignKeyConstraints;
    }

    /**
     * Get the index that has the given column as the first element.
     * This method returns null if no matching index is found.
     *
     * @param column the column
     * @param needGetFirstOrLast if the returned index must be able
     *          to do {@link Index#canGetFirstOrLast()}
     * @param needFindNext if the returned index must be able to do
     *          {@link Index#findNext(SessionLocal, SearchRow, SearchRow)}
     * @return the index or null
     */
    public Index getIndexForColumn(Column column,
            boolean needGetFirstOrLast, boolean needFindNext) {
        ArrayList<Index> indexes = getIndexes();
        Index result = null;
        if (indexes != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (needGetFirstOrLast && !index.canGetFirstOrLast()) {
                    continue;
                }
                if (needFindNext && !index.canFindNext()) {
                    continue;
                }
                // choose the minimal covering index with the needed first
                // column to work consistently with execution plan from
                // Optimizer
                if (index.isFirstColumn(column) && (result == null ||
                        result.getColumns().length > index.getColumns().length)) {
                    result = index;
                }
            }
        }
        return result;
    }

    public boolean getOnCommitDrop() {
        return onCommitDrop;
    }

    public void setOnCommitDrop(boolean onCommitDrop) {
        this.onCommitDrop = onCommitDrop;
    }

    public boolean getOnCommitTruncate() {
        return onCommitTruncate;
    }

    public void setOnCommitTruncate(boolean onCommitTruncate) {
        this.onCommitTruncate = onCommitTruncate;
    }

    /**
     * If the index is still required by a constraint, transfer the ownership to
     * it. Otherwise, the index is removed.
     *
     * @param session the session
     * @param index the index that is no longer required
     */
    public void removeIndexOrTransferOwnership(SessionLocal session, Index index) {
        boolean stillNeeded = false;
        if (constraints != null) {
            for (Constraint cons : constraints) {
                if (cons.usesIndex(index)) {
                    cons.setIndexOwner(index);
                    database.updateMeta(session, cons);
                    stillNeeded = true;
                }
            }
        }
        if (!stillNeeded) {
            database.removeSchemaObject(session, index);
        }
    }

    /**
     * Removes dependencies of column expressions, used for tables with circular
     * dependencies.
     *
     * @param session the session
     */
    public void removeColumnExpressionsDependencies(SessionLocal session) {
        for (Column column : columns) {
            column.setDefaultExpression(session, null);
            column.setOnUpdateExpression(session, null);
        }
    }

    /**
     * Check if a deadlock occurred. This method is called recursively. There is
     * a circle if the session to be tested has already being visited. If this
     * session is part of the circle (if it is the clash session), the method
     * must return an empty object array. Once a deadlock has been detected, the
     * methods must add the session to the list. If this session is not part of
     * the circle, or if no deadlock is detected, this method returns null.
     *
     * @param session the session to be tested for
     * @param clash set with sessions already visited, and null when starting
     *            verification
     * @param visited set with sessions already visited, and null when starting
     *            verification
     * @return an object array with the sessions involved in the deadlock, or
     *         null
     */
    @SuppressWarnings("unused")
    public ArrayList<SessionLocal> checkDeadlock(SessionLocal session, SessionLocal clash,
            Set<SessionLocal> visited) {
        return null;
    }

    public boolean isPersistIndexes() {
        return persistIndexes;
    }

    public boolean isPersistData() {
        return persistData;
    }

    /**
     * Compare two values with the current comparison mode. The values may be of
     * different type.
     *
     * @param provider the cast information provider
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareValues(CastDataProvider provider, Value a, Value b) {
        return a.compareTo(b, provider, compareMode);
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    /**
     * Tests if the table can be written. Usually, this depends on the
     * database.checkWritingAllowed method, but some tables (eg. TableLink)
     * overwrite this default behaviour.
     */
    public void checkWritingAllowed() {
        database.checkWritingAllowed();
    }

    @Override
    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        this.isHidden = hidden;
    }

    /**
     * Views, function tables, links, etc. do not support locks
     * @return true if table supports row-level locks
     */
    public boolean isRowLockable() {
        return false;
    }

    public void setTableExpression(boolean tableExpression) {
        this.tableExpression = tableExpression;
    }

    public boolean isTableExpression() {
        return tableExpression;
    }

    /**
     * Return list of triggers.
     *
     * @return list of triggers
     */
    public ArrayList<TriggerObject> getTriggers() {
        return triggers;
    }

    /**
     * Returns ID of main index column, or {@link SearchRow#ROWID_INDEX}.
     *
     * @return ID of main index column, or {@link SearchRow#ROWID_INDEX}
     */
    public int getMainIndexColumn() {
        return SearchRow.ROWID_INDEX;
    }

}
