/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.result.SearchRow;
import org.h2.result.SimpleRow;
import org.h2.result.SimpleRowValue;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObjectBase;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This is the base class for most tables.
 * A table contains a list of columns and a list of rows.
 */
public abstract class Table extends SchemaObjectBase {

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_CACHED = 0;

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_MEMORY = 1;

    /**
     * The table type name for linked tables.
     */
    public static final String TABLE_LINK = "TABLE LINK";

    /**
     * The table type name for system tables.
     */
    public static final String SYSTEM_TABLE = "SYSTEM TABLE";

    /**
     * The table type name for regular data tables.
     */
    public static final String TABLE = "TABLE";

    /**
     * The table type name for views.
     */
    public static final String VIEW = "VIEW";

    /**
     * The columns of this table.
     */
    protected Column[] columns;

    /**
     * The amount of memory required for a row if all values would be very small.
     */
    protected int memoryPerRow;

    /**
     * The compare mode used for this table.
     */
    protected CompareMode compareMode;

    private final HashMap<String, Column> columnMap = New.hashMap();
    private boolean persistIndexes;
    private boolean persistData;
    private ObjectArray<TriggerObject> triggers;
    private ObjectArray<Constraint> constraints;
    private ObjectArray<Sequence> sequences;
    private ObjectArray<TableView> views;
    private boolean checkForeignKeyConstraints = true;
    private boolean onCommitDrop, onCommitTruncate;
    private Row nullRow;

    Table(Schema schema, int id, String name, boolean persistIndexes, boolean persistData) {
        initSchemaObjectBase(schema, id, name, Trace.TABLE);
        this.persistIndexes = persistIndexes;
        this.persistData = persistData;
        compareMode = schema.getDatabase().getCompareMode();
    }

    public void rename(String newName) throws SQLException {
        super.rename(newName);
        for (int i = 0; constraints != null && i < constraints.size(); i++) {
            Constraint constraint = constraints.get(i);
            constraint.rebuild();
        }
    }

    /**
     * Lock the table for the given session.
     * This method waits until the lock is granted.
     *
     * @param session the session
     * @param exclusive true for write locks, false for read locks
     * @param force lock even in the MVCC mode
     * @throws SQLException if a lock timeout occurred
     */
    public abstract void lock(Session session, boolean exclusive, boolean force) throws SQLException;

    /**
     * Close the table object and flush changes.
     *
     * @param session the session
     */
    public abstract void close(Session session) throws SQLException;

    /**
     * Release the lock for this session.
     *
     * @param s the session
     */
    public abstract void unlock(Session s);

    /**
     * Create an index for this table
     *
     * @param session the session
     * @param indexName the name of the index
     * @param indexId the id
     * @param cols the index columns
     * @param indexType the index type
     * @param headPos the position of the head (if the index already exists)
     * @param comment the comment
     * @return the index
     */
    public abstract Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException;

    /**
     * Remove a row from the table and all indexes.
     *
     * @param session the session
     * @param row the row
     */
    public abstract void removeRow(Session session, Row row) throws SQLException;

    /**
     * Remove all rows from the table and indexes.
     *
     * @param session the session
     */
    public abstract void truncate(Session session) throws SQLException;

    /**
     * Add a row to the table and all indexes.
     *
     * @param session the session
     * @param row the row
     * @throws SQLException if a constraint was violated
     */
    public abstract void addRow(Session session, Row row) throws SQLException;

    /**
     * Check if this table supports ALTER TABLE.
     *
     * @throws SQLException if it is not supported
     */
    public abstract void checkSupportAlter() throws SQLException;

    /**
     * Get the table type name
     *
     * @return the table type name
     */
    public abstract String getTableType();

    /**
     * Get the scan index to iterate through all rows.
     *
     * @param session the session
     * @return the index
     */
    public abstract Index getScanIndex(Session session) throws SQLException;

    /**
     * Get any unique index for this table if one exists.
     *
     * @return a unique index
     */
    public abstract Index getUniqueIndex();

    /**
     * Get all indexes for this table.
     *
     * @return the list of indexes
     */
    public abstract ObjectArray<Index> getIndexes();

    /**
     * Check if this table is locked exclusively.
     *
     * @return true if it is.
     */
    public abstract boolean isLockedExclusively();

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
     * @return true if it can
     */
    public abstract boolean canGetRowCount();

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
    public abstract long getRowCount(Session session) throws SQLException;

    /**
     * Get the approximated row count for this table.
     *
     * @return the approximated row count
     */
    public abstract long getRowCountApproximation();

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.throwInternalError();
    }

    /**
     * Add all objects that this table depends on to the hash set.
     *
     * @param dependencies the current set of dependencies
     */
    public void addDependencies(HashSet<DbObject> dependencies) {
        if (sequences != null) {
            for (Sequence s : sequences) {
                dependencies.add(s);
            }
        }
        ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.GET_DEPENDENCIES);
        visitor.setDependencies(dependencies);
        for (Column col : columns) {
            col.isEverything(visitor);
        }
    }

    public ObjectArray<DbObject> getChildren() {
        ObjectArray<DbObject> children = ObjectArray.newInstance();
        ObjectArray<Index> indexes = getIndexes();
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
        if (views != null) {
            children.addAll(views);
        }
        ObjectArray<Right> rights = database.getAllRights();
        for (Right right : rights) {
            if (right.getGrantedTable() == this) {
                children.add(right);
            }
        }
        return children;
    }

    protected void setColumns(Column[] columns) throws SQLException {
        this.columns = columns;
        if (columnMap.size() > 0) {
            columnMap.clear();
        }
        int memory = 0;
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            int dataType = col.getType();
            if (dataType == Value.UNKNOWN) {
                throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, col.getSQL());
            }
            memory += DataType.getDataType(dataType).memory;
            col.setTable(this, i);
            String columnName = col.getName();
            if (columnMap.get(columnName) != null) {
                throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, columnName);
            }
            columnMap.put(columnName, col);
        }
        memoryPerRow = memory;
    }

    /**
     * Rename a column of this table.
     *
     * @param column the column to rename
     * @param newName the new column name
     */
    public void renameColumn(Column column, String newName) throws SQLException {
        for (Column c : columns) {
            if (c == column) {
                continue;
            }
            if (c.getName().equals(newName)) {
                throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, newName);
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
    boolean isLockedExclusivelyBy(Session session) {
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
    public void updateRows(Prepared prepared, Session session, RowList rows)
            throws SQLException {
        // remove the old rows
        int rowScanCount = 0;
        for (rows.reset(); rows.hasNext();) {
            if ((rowScanCount++ & 127) == 0) {
                prepared.checkCanceled();
            }
            Row o = rows.next();
            rows.next();
            removeRow(session, o);
            session.log(this, UndoLogRecord.DELETE, o);
        }
        // add the new rows
        for (rows.reset(); rows.hasNext();) {
            if ((rowScanCount++ & 127) == 0) {
                prepared.checkCanceled();
            }
            rows.next();
            Row n = rows.next();
            addRow(session, n);
            session.log(this, UndoLogRecord.INSERT, n);
        }
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        while (views != null && views.size() > 0) {
            TableView view = views.get(0);
            views.remove(0);
            database.removeSchemaObject(session, view);
        }
        while (triggers != null && triggers.size() > 0) {
            TriggerObject trigger = triggers.get(0);
            triggers.remove(0);
            database.removeSchemaObject(session, trigger);
        }
        while (constraints != null && constraints.size() > 0) {
            Constraint constraint = constraints.get(0);
            constraints.remove(0);
            database.removeSchemaObject(session, constraint);
        }
        for (Right right : database.getAllRights()) {
            if (right.getGrantedTable() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        // must delete sequences later (in case there is a power failure
        // before removing the table object)
        while (sequences != null && sequences.size() > 0) {
            Sequence sequence = sequences.get(0);
            sequences.remove(0);
            if (!isTemporary()) {
                // only remove if no other table depends on this sequence
                // this is possible when calling ALTER TABLE ALTER COLUMN
                if (database.getDependentTable(sequence, this) == null) {
                    database.removeSchemaObject(session, sequence);
                }
            }
        }
    }

    /**
     * Check that this column is not referenced by a referential constraint or
     * multi-column index.
     *
     * @param col the column
     * @throws SQLException if the column is referenced
     */
    public void checkColumnIsNotReferenced(Column col) throws SQLException {
        for (int i = 0; constraints != null && i < constraints.size(); i++) {
            Constraint constraint = constraints.get(i);
            if (constraint.containsColumn(col)) {
                throw Message.getSQLException(ErrorCode.COLUMN_MAY_BE_REFERENCED_1, constraint.getSQL());
            }
        }
        ObjectArray<Index> indexes = getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            Index index = indexes.get(i);
            if (index.getColumns().length == 1) {
                continue;
            }
            if (index.getCreateSQL() == null) {
                continue;
            }
            if (index.getColumnIndex(col) >= 0) {
                throw Message.getSQLException(ErrorCode.COLUMN_MAY_BE_REFERENCED_1, index.getSQL());
            }
        }
    }

    public Row getTemplateRow() {
        return new Row(new Value[columns.length], memoryPerRow);
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
        return new SimpleRow(new Value[columns.length]);
    }

    Row getNullRow() {
        synchronized (this) {
            if (nullRow == null) {
                nullRow = new Row(new Value[columns.length], 0);
                for (int i = 0; i < columns.length; i++) {
                    nullRow.setValue(i, ValueNull.INSTANCE);
                }
            }
            return nullRow;
        }
    }

    public Column[] getColumns() {
        return columns;
    }

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
     * @throws SQLException if the column was not found
     */
    public Column getColumn(String columnName) throws SQLException {
        Column column = columnMap.get(columnName);
        if (column == null) {
            throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks null means 'always false'
     * @return the plan item
     */
    public PlanItem getBestPlanItem(Session session, int[] masks) throws SQLException {
        PlanItem item = new PlanItem();
        item.setIndex(getScanIndex(session));
        item.cost = item.getIndex().getCost(session, null);
        ObjectArray<Index> indexes = getIndexes();
        for (int i = 1; indexes != null && masks != null && i < indexes.size(); i++) {
            Index index = indexes.get(i);
            double cost = index.getCost(session, masks);
            if (cost < item.cost) {
                item.cost = cost;
                item.setIndex(index);
            }
        }
        return item;
    }

    /**
     * Get the primary key index if there is one, or null if there is none.
     *
     * @return the primary key index or null
     */
    public Index findPrimaryKey() {
        ObjectArray<Index> indexes = getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            Index idx = indexes.get(i);
            if (idx.getIndexType().isPrimaryKey()) {
                return idx;
            }
        }
        return null;
    }

    public Index getPrimaryKey() throws SQLException {
        Index index = findPrimaryKey();
        if (index != null) {
            return index;
        }
        throw Message.getSQLException(ErrorCode.INDEX_NOT_FOUND_1, Constants.PREFIX_PRIMARY_KEY);
    }

    /**
     * Validate all values in this row, convert the values if required, and
     * update the sequence values if required. This call will also set the
     * default values if required and set the computed column if there are any.
     *
     * @param session the session
     * @param row the row
     */
    public void validateConvertUpdateSequence(Session session, Row row) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            Value v2;
            if (column.getComputed()) {
                // force updating the value
                value = null;
                v2 = column.computeValue(session, row);
            }
            v2 = column.validateConvertUpdateSequence(session, value);
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
    }

    private void remove(ObjectArray< ? extends DbObject> list, DbObject obj) {
        if (list != null) {
            int i = list.indexOf(obj);
            if (i >= 0) {
                list.remove(i);
            }
        }
    }

    /**
     * Remove the given index from the list.
     *
     * @param index the index to remove
     */
    public void removeIndex(Index index) {
        ObjectArray<Index> indexes = getIndexes();
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
     * Remove the given view from the list.
     *
     * @param view the view to remove
     */
    void removeView(TableView view) {
        remove(views, view);
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
     * @param session the session
     * @param sequence the sequence to remove
     */
    public void removeSequence(Session session, Sequence sequence) {
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
    public void addView(TableView view) {
        views = add(views, view);
    }

    /**
     * Add a constraint to the table.
     *
     * @param constraint the constraint to add
     */
    public void addConstraint(Constraint constraint) {
        if (constraints == null || constraints.indexOf(constraint) < 0) {
            constraints = add(constraints, constraint);
        }
    }

    public ObjectArray<Constraint> getConstraints() {
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

    private <T> ObjectArray<T> add(ObjectArray<T> list, T obj) {
        if (list == null) {
            list = ObjectArray.newInstance();
        }
        // self constraints are two entries in the list
        list.add(obj);
        return list;
    }

    /**
     * Fire the before update triggers for this table.
     *
     * @param session the session
     */
    public void fireBefore(Session session) throws SQLException {
        // TODO trigger: for sql server compatibility,
        // should send list of rows, not just 'the event'
        fire(session, true);
    }

    /**
     * Fire the after update triggers for this table.
     *
     * @param session the session
     */
    public void fireAfter(Session session) throws SQLException {
        fire(session, false);
    }

    private void fire(Session session, boolean beforeAction) throws SQLException {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                trigger.fire(session, beforeAction);
            }
        }
    }

    /**
     * Check if row based triggers or constraints are defined.
     * In this case the fire after and before row methods need to be called.
     *
     *  @return if there are any triggers or rows defined
     */
    public boolean fireRow() {
        return (constraints != null && constraints.size() > 0) || (triggers != null && triggers.size() > 0);
    }

    /**
     * Fire all triggers that need to be called before a row is updated.
     *
     *  @param session the session
     *  @param oldRow the old data or null for an insert
     *  @param newRow the new data or null for a delete
     */
    public void fireBeforeRow(Session session, Row oldRow, Row newRow) throws SQLException {
        fireRow(session, oldRow, newRow, true);
        fireConstraints(session, oldRow, newRow, true);
    }

    private void fireConstraints(Session session, Row oldRow, Row newRow, boolean before) throws SQLException {
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
     */
    public void fireAfterRow(Session session, Row oldRow, Row newRow) throws SQLException {
        fireRow(session, oldRow, newRow, false);
        fireConstraints(session, oldRow, newRow, false);
    }

    private void fireRow(Session session, Row oldRow, Row newRow, boolean beforeAction) throws SQLException {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                trigger.fireRow(session, oldRow, newRow, beforeAction);
            }
        }
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
     * @param checkExisting true if existing rows must be checked during this call
     */
    public void setCheckForeignKeyConstraints(Session session, boolean enabled, boolean checkExisting)
            throws SQLException {
        if (enabled && checkExisting) {
            for (int i = 0; constraints != null && i < constraints.size(); i++) {
                Constraint c = constraints.get(i);
                c.checkExistingData(session);
            }
        }
        checkForeignKeyConstraints = enabled;
    }

    public boolean getCheckForeignKeyConstraints() {
        return checkForeignKeyConstraints;
    }

    /**
     * Get the index that has the given column as the first element.
     * This method returns null if no matching index is found.
     *
     * @param column the column
     * @param first if the min value should be returned
     * @return the index or null
     */
    public Index getIndexForColumn(Column column, boolean first) {
        ObjectArray<Index> indexes = getIndexes();
        for (int i = 1; indexes != null && i < indexes.size(); i++) {
            Index index = indexes.get(i);
            if (index.canGetFirstOrLast()) {
                int idx = index.getColumnIndex(column);
                if (idx == 0) {
                    return index;
                }
            }
        }
        return null;
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

    boolean getClustered() {
        return false;
    }

    /**
     * If the index is still required by a constraint, transfer the ownership to
     * it. Otherwise, the index is removed.
     *
     * @param session the session
     * @param index the index that is no longer required
     */
    public void removeIndexOrTransferOwnership(Session session, Index index) throws SQLException {
        boolean stillNeeded = false;
        for (int i = 0; constraints != null && i < constraints.size(); i++) {
            Constraint cons = constraints.get(i);
            if (cons.usesIndex(index)) {
                cons.setIndexOwner(index);
                database.update(session, cons);
                stillNeeded = true;
            }
        }
        if (!stillNeeded) {
            database.removeSchemaObject(session, index);
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
    public ObjectArray<Session> checkDeadlock(Session session, Session clash, Set<Session> visited) {
        return null;
    }

    public boolean isPersistIndexes() {
        return persistIndexes;
    }

    public boolean isPersistData() {
        return persistData;
    }

    /**
     * Compare two values with the current comparison mode. The values must be
     * of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSave(Value a, Value b) throws SQLException {
        return a.compareTypeSave(b, compareMode);
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

}
