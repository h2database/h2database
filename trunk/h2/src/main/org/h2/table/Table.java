/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import java.util.HashMap;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SimpleRow;
import org.h2.result.SimpleRowValue;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObjectBase;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.UndoLogRecord;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * @author Thomas
 */

public abstract class Table extends SchemaObjectBase {
    
    public static final int TYPE_CACHED = 0, TYPE_MEMORY = 1;
    
    public static final String TABLE_LINK = "TABLE LINK";
    public static final String SYSTEM_TABLE = "SYSTEM TABLE";
    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";

    protected Column[] columns;
    private final HashMap columnMap = new HashMap();
    private final boolean persistent;
    private ObjectArray triggers;
    private ObjectArray constraints;
    private ObjectArray sequences;
    private ObjectArray views;
    private boolean checkForeignKeyConstraints = true;
    private boolean onCommitDrop, onCommitTruncate;
    protected int memoryPerRow;

    public Table(Schema schema, int id, String name, boolean persistent) {
        super(schema, id, name, Trace.TABLE);
        this.persistent = persistent;
    }
    
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.getInternalError();
    }    
    
    public ObjectArray getChildren() {
        ObjectArray children = new ObjectArray();
        ObjectArray indexes = getIndexes();
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
        ObjectArray rights = database.getAllRights();
        for (int i = 0; i < rights.size(); i++) {
            Right right = (Right) rights.get(i);
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
            memory += DataType.getDataType(col.getType()).memory;
            col.setTable(this, i);
            String columnName = col.getName();
            if (columnMap.get(columnName) != null) {
                throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, columnName);
            }
            columnMap.put(columnName, col);
        }
        memoryPerRow = memory;
    }

    public void renameColumn(Column column, String newName) {
        columnMap.remove(column.getName());
        column.rename(newName);
        columnMap.put(newName, column);
    }

    public abstract void lock(Session session, boolean exclusive, boolean force) throws SQLException;

    public abstract void close(Session session) throws SQLException;

    public abstract void unlock(Session s);

    public abstract Index addIndex(Session session, String indexName, int indexId, Column[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException;

    public abstract void removeRow(Session session, Row row) throws SQLException;

    public abstract void truncate(Session session) throws SQLException;

    public abstract void addRow(Session session, Row row) throws SQLException;

    public abstract void checkSupportAlter() throws SQLException;

    public abstract String getTableType();

    public abstract Index getScanIndex(Session session) throws SQLException;

    public abstract Index getUniqueIndex();

    public abstract ObjectArray getIndexes();

    public abstract boolean isLockedExclusively();

    public abstract long getMaxDataModificationId();

    public void updateRows(Prepared prepared, Session session, ObjectArray oldRows, ObjectArray newRows)
            throws SQLException {
        // remove the old rows
        for (int i = 0; i < oldRows.size(); i++) {
            prepared.checkCancelled();
            Row o = (Row) oldRows.get(i);
            removeRow(session, o);
            session.log(this, UndoLogRecord.DELETE, o);
        }
        // add the new rows
        for (int i = 0; i < newRows.size(); i++) {
            prepared.checkCancelled();
            Row n = (Row) newRows.get(i);
            addRow(session, n);
            session.log(this, UndoLogRecord.INSERT, n);
        }
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        while (views != null && views.size() > 0) {
            TableView view = (TableView) views.get(0);
            views.remove(0);
            database.removeSchemaObject(session, view);
        }
        while (triggers != null && triggers.size() > 0) {
            TriggerObject trigger = (TriggerObject) triggers.get(0);
            triggers.remove(0);
            database.removeSchemaObject(session, trigger);
        }
        while (constraints != null && constraints.size() > 0) {
            Constraint constraint = (Constraint) constraints.get(0);
            constraints.remove(0);
            database.removeSchemaObject(session, constraint);
        }
        while (sequences != null && sequences.size() > 0) {
            Sequence sequence = (Sequence) sequences.get(0);
            sequences.remove(0);
            if (!getTemporary()) {
                database.removeSchemaObject(session, sequence);
            }
        }
        ObjectArray rights = database.getAllRights();
        for (int i = 0; i < rights.size(); i++) {
            Right right = (Right) rights.get(i);
            if (right.getGrantedTable() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
    }

    public void checkColumnIsNotReferenced(Column col) throws SQLException {
        for (int i = 0; constraints != null && i < constraints.size(); i++) {
            Constraint constraint = (Constraint) constraints.get(i);
            if (constraint.containsColumn(col)) {
                throw Message.getSQLException(ErrorCode.COLUMN_MAY_BE_REFERENCED_1, constraint.getSQL());
            }
        }
        ObjectArray indexes = getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
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

    public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        if (singleColumn) {
            return new SimpleRowValue(columns.length);
        } else {
            return new SimpleRow(new Value[columns.length]);
        }
    }

    public Row getNullRow() {
        // TODO memory usage: if rows are immutable, we could use a static null
        // row
        Row row = new Row(new Value[columns.length], 0);
        for (int i = 0; i < columns.length; i++) {
            row.setValue(i, ValueNull.INSTANCE);
        }
        return row;
    }

    public Column[] getColumns() {
        return columns;
    }

    public int getType() {
        return DbObject.TABLE_OR_VIEW;
    }

    public Column getColumn(int index) {
        return columns[index];
    }

    public Column getColumn(String columnName) throws SQLException {
        Column column = (Column) columnMap.get(columnName);
        if (column == null) {
            throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }
    
    /**
     * @param masks - null means 'always false'
     */
    public PlanItem getBestPlanItem(Session session, int[] masks) throws SQLException {
        PlanItem item = new PlanItem();
        item.setIndex(getScanIndex(session));
        item.cost = item.getIndex().getCost(session, null);
        ObjectArray indexes = getIndexes();               
        for (int i = 1; indexes != null && masks != null && i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            double cost = index.getCost(session, masks);
            if (cost < item.cost) {
                item.cost = cost;
                item.setIndex(index);
            }
        }
        return item;
    }
    
    public Index findPrimaryKey() throws SQLException {
        ObjectArray indexes = getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            Index idx = (Index) indexes.get(i);
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
        throw Message.getSQLException(ErrorCode.INDEX_NOT_FOUND_1, Constants.PRIMARY_KEY_PREFIX);
    }

    public void validateConvertUpdateSequence(Session session, Row row) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            Value v2;
            if (column.getComputed()) {
                v2 = column.computeValue(session, row);
            } else {
                v2 = column.validateConvertUpdateSequence(session, value);
            }
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
    }

    public boolean isPersistent() {
        return persistent;
    }

    private void remove(ObjectArray list, DbObject obj) {
        if (list != null) {
            int i = list.indexOf(obj);
            if (i >= 0) {
                list.remove(i);
            }
        }
    }

    public void removeIndex(Index index) {
        ObjectArray indexes = getIndexes();
        if (indexes != null) {
            remove(indexes, index);
            if (index.getIndexType().isPrimaryKey()) {
                Column[] cols = index.getColumns();
                for (int i = 0; i < cols.length; i++) {
                    cols[i].setPrimaryKey(false);
                }
            }
        }
    }

    public void removeView(TableView view) {
        remove(views, view);
    }

    public void removeConstraint(Constraint constraint) {
        remove(constraints, constraint);
    }

    public void removeSequence(Session session, Sequence sequence) {
        remove(sequences, sequence);
    }

    public void removeTrigger(Session session, TriggerObject trigger) {
        remove(triggers, trigger);
    }

    public void addView(TableView view) {
        views = add(views, view);
    }

    public void addConstraint(Constraint constraint) {
        if (constraints == null || constraints.indexOf(constraint) < 0) {
            constraints = add(constraints, constraint);
        }
    }

    public ObjectArray getConstraints() {
        return constraints;
    }

    public void addSequence(Sequence sequence) {
        sequences = add(sequences, sequence);
    }

    public void addTrigger(TriggerObject trigger) {
        triggers = add(triggers, trigger);
    }

    private ObjectArray add(ObjectArray list, DbObject obj) {
        if (list == null) {
            list = new ObjectArray();
        }
        // self constraints are two entries in the list
//        if(Database.CHECK) {
//            if(list.indexOf(obj) >= 0) {
//                throw Message.internal("object already in list: " + obj.getName());
//            }
//        }
        list.add(obj);
        return list;
    }

    public void fireBefore(Session session) throws SQLException {
        // TODO trigger: for sql server compatibility, should send list of rows, not just 'the event'
        fire(session, true);
    }

    public void fireAfter(Session session) throws SQLException {
        fire(session, false);
    }

    private void fire(Session session, boolean beforeAction) throws SQLException {
        if (triggers != null) {
            for (int i = 0; i < triggers.size(); i++) {
                TriggerObject trigger = (TriggerObject) triggers.get(i);
                trigger.fire(session, beforeAction);
            }
        }
    }

    public boolean fireRow() {
        return (constraints != null && constraints.size() > 0) || (triggers != null && triggers.size() > 0);
    }

    public void fireBeforeRow(Session session, Row oldRow, Row newRow) throws SQLException {
        fireRow(session, oldRow, newRow, true);
        fireConstraints(session, oldRow, newRow, true);
    }

    private void fireConstraints(Session session, Row oldRow, Row newRow, boolean before) throws SQLException {
        if (constraints != null) {
            for (int i = 0; i < constraints.size(); i++) {
                Constraint constraint = (Constraint) constraints.get(i);
                if (constraint.isBefore() == before) {
                    constraint.checkRow(session, this, oldRow, newRow);
                }
            }
        }
    }

    public void fireAfterRow(Session session, Row oldRow, Row newRow) throws SQLException {
        fireRow(session, oldRow, newRow, false);
        fireConstraints(session, oldRow, newRow, false);
    }

    private void fireRow(Session session, Row oldRow, Row newRow, boolean beforeAction) throws SQLException {
        if (triggers != null) {
            for (int i = 0; i < triggers.size(); i++) {
                TriggerObject trigger = (TriggerObject) triggers.get(i);
                trigger.fireRow(session, oldRow, newRow, beforeAction);
            }
        }
    }

    public Column[] getColumns(String[] columnNames) throws SQLException {
        Column[] cols = new Column[columnNames.length];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = getColumn(columnNames[i]);
        }
        return cols;
    }

    public abstract boolean canGetRowCount();

    public abstract boolean canDrop();

    public abstract long getRowCount(Session session) throws SQLException;

    public boolean getGlobalTemporary() {
        return false;
    }

    public boolean canTruncate() {
        return false;
    }

    public void setCheckForeignKeyConstraints(Session session, boolean enabled, boolean checkExisting)
            throws SQLException {
        if (enabled && checkExisting) {
            for (int i = 0; constraints != null && i < constraints.size(); i++) {
                Constraint c = (Constraint) constraints.get(i);
                c.checkExistingData(session);
            }
        }
        checkForeignKeyConstraints = enabled;
    }

    public boolean getCheckForeignKeyConstraints() {
        return checkForeignKeyConstraints;
    }

    public Index getIndexForColumn(Column column, boolean first) {
        ObjectArray indexes = getIndexes();
        for (int i = 1; indexes != null && i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            if (index.canGetFirstOrLast(first)) {
                int idx = index.getColumnIndex(column);
                if (idx == 0) {
                    return index;
                }
            }
        }
        return null;
    }

    public boolean isOnCommitDrop() {
        return onCommitDrop;
    }

    public void setOnCommitDrop(boolean onCommitDrop) {
        this.onCommitDrop = onCommitDrop;
    }

    public boolean isOnCommitTruncate() {
        return onCommitTruncate;
    }

    public void setOnCommitTruncate(boolean onCommitTruncate) {
        this.onCommitTruncate = onCommitTruncate;
    }

    public boolean isClustered() {
        return false;
    }

}
