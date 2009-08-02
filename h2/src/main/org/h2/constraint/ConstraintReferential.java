/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A referential constraint.
 */
public class ConstraintReferential extends Constraint {

    /**
     * The action is to restrict the operation.
     */
    public static final int RESTRICT = 0;

    /**
     * The action is to cascade the operation.
     */
    public static final int CASCADE = 1;

    /**
     * The action is to set the value to the default value.
     */
    public static final int SET_DEFAULT = 2;

    /**
     * The action is to set the value to NULL.
     */
    public static final int SET_NULL = 3;

    private IndexColumn[] columns;
    private IndexColumn[] refColumns;
    private int deleteAction;
    private int updateAction;
    private Table refTable;
    private Index index;
    private Index refIndex;
    private boolean indexOwner;
    private boolean refIndexOwner;
    private String deleteSQL, updateSQL;
    private boolean skipOwnTable;

    public ConstraintReferential(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    public String getConstraintType() {
        return Constraint.REFERENTIAL;
    }

    private void appendAction(StatementBuilder buff, int action) {
        switch (action) {
        case CASCADE:
            buff.append("CASCADE");
            break;
        case SET_DEFAULT:
            buff.append("SET DEFAULT");
            break;
        case SET_NULL:
            buff.append("SET NULL");
            break;
        default:
            Message.throwInternalError("action=" + action);
        }
    }

    /**
     * Create the SQL statement of this object so a copy of the table can be made.
     *
     * @param forTable the table to create the object for
     * @param quotedName the name of this object (quoted if necessary)
     * @return the SQL statement
     */
    public String getCreateSQLForCopy(Table forTable, String quotedName) {
        return getCreateSQLForCopy(forTable, refTable, quotedName, true);
    }

    /**
     * Create the SQL statement of this object so a copy of the table can be made.
     *
     * @param forTable the table to create the object for
     * @param forRefTable the referenced table
     * @param quotedName the name of this object (quoted if necessary)
     * @param internalIndex add the index name to the statement
     * @return the SQL statement
     */
    public String getCreateSQLForCopy(Table forTable, Table forRefTable, String quotedName, boolean internalIndex) {
        StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
        String mainTable = forTable.getSQL();
        buff.append(mainTable).append(" ADD CONSTRAINT ").append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        IndexColumn[] cols = columns;
        IndexColumn[] refCols = refColumns;
        buff.append(" FOREIGN KEY(");
        for (IndexColumn c : cols) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');
        if (internalIndex && indexOwner && forTable == this.table) {
            buff.append(" INDEX ").append(index.getSQL());
        }
        buff.append(" REFERENCES ");
        String quotedRefTable;
        if (this.table == this.refTable) {
            // self-referencing constraints: need to use new table
            quotedRefTable = forTable.getSQL();
        } else {
            quotedRefTable = forRefTable.getSQL();
        }
        buff.append(quotedRefTable).append('(');
        buff.resetCount();
        for (IndexColumn r : refCols) {
            buff.appendExceptFirst(", ");
            buff.append(r.getSQL());
        }
        buff.append(')');
        if (internalIndex && refIndexOwner && forTable == this.table) {
            buff.append(" INDEX ").append(refIndex.getSQL());
        }
        if (deleteAction != RESTRICT) {
            buff.append(" ON DELETE ");
            appendAction(buff, deleteAction);
        }
        if (updateAction != RESTRICT) {
            buff.append(" ON UPDATE ");
            appendAction(buff, updateAction);
        }
        return buff.append(" NOCHECK").toString();
    }


    /**
     * Get a short description of the constraint. This includes the constraint
     * name (if set), and the constraint expression.
     *
     * @return the description
     */
    public String getShortDescription() {
        StatementBuilder buff = new StatementBuilder(getName());
        buff.append(": ").append(table.getSQL()).append(" FOREIGN KEY(");
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(") REFERENCES ").append(refTable.getSQL()).append('(');
        buff.resetCount();
        for (IndexColumn r : refColumns) {
            buff.appendExceptFirst(", ");
            buff.append(r.getSQL());
        }
        return buff.append(')').toString();
    }

    public String getCreateSQLWithoutIndexes() {
        return getCreateSQLForCopy(table, refTable, getSQL(), false);
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public void setColumns(IndexColumn[] cols) {
        columns = cols;
    }

    public IndexColumn[] getColumns() {
        return columns;
    }

    public void setRefColumns(IndexColumn[] refCols) {
        refColumns = refCols;
    }

    public IndexColumn[] getRefColumns() {
        return refColumns;
    }

    public void setRefTable(Table refTable) {
        this.refTable = refTable;
        if (refTable.isTemporary()) {
            setTemporary(true);
        }
    }

    /**
     * Set the index to use for this constraint.
     *
     * @param index the index
     * @param isOwner true if the index is generated by the system and belongs
     *            to this constraint
     */
    public void setIndex(Index index, boolean isOwner) {
        this.index = index;
        this.indexOwner = isOwner;
    }

    /**
     * Set the index of the referenced table to use for this constraint.
     *
     * @param refIndex the index
     * @param isRefOwner true if the index is generated by the system and
     *            belongs to this constraint
     */
    public void setRefIndex(Index refIndex, boolean isRefOwner) {
        this.refIndex = refIndex;
        this.refIndexOwner = isRefOwner;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        table.removeConstraint(this);
        refTable.removeConstraint(this);
        if (indexOwner) {
            table.removeIndexOrTransferOwnership(session, index);
        }
        if (refIndexOwner) {
            refTable.removeIndexOrTransferOwnership(session, refIndex);
        }
        database.removeMeta(session, getId());
        refTable = null;
        index = null;
        refIndex = null;
        columns = null;
        refColumns = null;
        deleteSQL = null;
        updateSQL = null;
        table = null;
        invalidate();
    }

    public void checkRow(Session session, Table t, Row oldRow, Row newRow) throws SQLException {
        if (!database.getReferentialIntegrity()) {
            return;
        }
        if (!table.getCheckForeignKeyConstraints() || !refTable.getCheckForeignKeyConstraints()) {
            return;
        }
        if (t == table) {
            if (!skipOwnTable) {
                checkRowOwnTable(session, oldRow, newRow);
            }
        }
        if (t == refTable) {
            checkRowRefTable(session, oldRow, newRow);
        }
    }

    private void checkRowOwnTable(Session session, Row oldRow, Row newRow) throws SQLException {
        if (newRow == null) {
            return;
        }
        boolean constraintColumnsEqual = oldRow != null;
        for (IndexColumn col : columns) {
            int idx = col.column.getColumnId();
            Value v = newRow.getValue(idx);
            if (v == ValueNull.INSTANCE) {
                // return early if one of the columns is NULL
                return;
            }
            if (constraintColumnsEqual) {
                if (!v.compareEqual(oldRow.getValue(idx))) {
                    constraintColumnsEqual = false;
                }
            }
        }
        if (constraintColumnsEqual) {
            // return early if the key columns didn't change
            return;
        }
        if (refTable == table) {
            // special case self referencing constraints:
            // check the inserted row first
            boolean self = true;
            for (int i = 0; i < columns.length; i++) {
                int idx = columns[i].column.getColumnId();
                Value v = newRow.getValue(idx);
                Column refCol = refColumns[i].column;
                int refIdx = refCol.getColumnId();
                Value r = newRow.getValue(refIdx);
                if (!database.areEqual(r, v)) {
                    self = false;
                    break;
                }
            }
            if (self) {
                return;
            }
        }
        Row check = refTable.getTemplateRow();
        for (int i = 0; i < columns.length; i++) {
            int idx = columns[i].column.getColumnId();
            Value v = newRow.getValue(idx);
            Column refCol = refColumns[i].column;
            int refIdx = refCol.getColumnId();
            check.setValue(refIdx, v.convertTo(refCol.getType()));
        }
        if (!found(session, refIndex, check, null)) {
            throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1,
                    getShortDescription());
        }
    }

    private boolean found(Session session, Index searchIndex, SearchRow check, Row excluding) throws SQLException {
        Table table = searchIndex.getTable();
        table.lock(session, false, false);
        Cursor cursor = searchIndex.find(session, check, check);
        while (cursor.next()) {
            SearchRow found;
            found = cursor.getSearchRow();
            if (excluding != null && found.getPos() == excluding.getPos()) {
                continue;
            }
            Column[] cols = searchIndex.getColumns();
            boolean allEqual = true;
            for (int i = 0; i < columns.length && i < cols.length; i++) {
                int idx = cols[i].getColumnId();
                Value c = check.getValue(idx);
                Value f = found.getValue(idx);
                if (table.compareTypeSave(c, f) != 0) {
                    allEqual = false;
                    break;
                }
            }
            if (allEqual) {
                return true;
            }
        }
        return false;
    }

    private boolean isEqual(Row oldRow, Row newRow) throws SQLException {
        return refIndex.compareRows(oldRow, newRow) == 0;
    }

    private void checkRow(Session session, Row oldRow) throws SQLException {
        SearchRow check = table.getTemplateSimpleRow(false);
        for (int i = 0; i < columns.length; i++) {
            Column refCol = refColumns[i].column;
            int refIdx = refCol.getColumnId();
            Column col = columns[i].column;
            int idx = col.getColumnId();
            Value v = oldRow.getValue(refIdx).convertTo(col.getType());
            check.setValue(idx, v);
        }
        // exclude the row only for self-referencing constraints
        Row excluding = (refTable == table) ? oldRow : null;
        if (found(session, index, check, excluding)) {
            throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1,
                    getShortDescription());
        }
    }

    private void checkRowRefTable(Session session, Row oldRow, Row newRow) throws SQLException {
        if (oldRow == null) {
            // this is an insert
            return;
        }
        if (newRow != null && isEqual(oldRow, newRow)) {
            // on an update, if both old and new are the same, don't do anything
            return;
        }
        if (newRow == null) {
            // this is a delete
            if (deleteAction == RESTRICT) {
                checkRow(session, oldRow);
            } else {
                int i = deleteAction == CASCADE ? 0 : columns.length;
                Prepared deleteCommand = getDelete(session);
                setWhere(deleteCommand, i, oldRow);
                updateWithSkipCheck(deleteCommand);
            }
        } else {
            // this is an update
            if (updateAction == RESTRICT) {
                checkRow(session, oldRow);
            } else {
                Prepared updateCommand = getUpdate(session);
                if (updateAction == CASCADE) {
                    ObjectArray<Parameter> params = updateCommand.getParameters();
                    for (int i = 0; i < columns.length; i++) {
                        Parameter param = params.get(i);
                        Column refCol = refColumns[i].column;
                        param.setValue(newRow.getValue(refCol.getColumnId()));
                    }
                }
                setWhere(updateCommand, columns.length, oldRow);
                updateWithSkipCheck(updateCommand);
            }
        }
    }

    private void updateWithSkipCheck(Prepared prep) throws SQLException {
        // TODO constraints: maybe delay the update or support delayed checks
        // (until commit)
        try {
            // TODO multithreaded kernel: this works only if nobody else updates
            // this or the ref table at the same time
            skipOwnTable = true;
            prep.update();
        } finally {
            skipOwnTable = false;
        }
    }

    private void setWhere(Prepared command, int pos, Row row) {
        for (int i = 0; i < refColumns.length; i++) {
            int idx = refColumns[i].column.getColumnId();
            Value v = row.getValue(idx);
            ObjectArray<Parameter> params = command.getParameters();
            Parameter param = params.get(pos + i);
            param.setValue(v);
        }
    }

    public int getDeleteAction() {
        return deleteAction;
    }

    /**
     * Set the action to apply (restrict, cascade,...) on a delete.
     *
     * @param action the action
     */
    public void setDeleteAction(int action) throws SQLException {
        if (action == deleteAction && deleteSQL == null) {
            return;
        }
        if (deleteAction != RESTRICT) {
            throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON DELETE");
        }
        this.deleteAction = action;
        buildDeleteSQL();
    }

    private void buildDeleteSQL() {
        if (deleteAction == RESTRICT) {
            return;
        }
        StatementBuilder buff = new StatementBuilder();
        if (deleteAction == CASCADE) {
            buff.append("DELETE FROM ").append(table.getSQL());
        } else {
            appendUpdate(buff);
        }
        appendWhere(buff);
        deleteSQL = buff.toString();
    }

    private Prepared getUpdate(Session session) throws SQLException {
        return prepare(session, updateSQL, updateAction);
    }

    private Prepared getDelete(Session session) throws SQLException {
        return prepare(session, deleteSQL, deleteAction);
    }

    public int getUpdateAction() {
        return updateAction;
    }

    /**
     * Set the action to apply (restrict, cascade,...) on an update.
     *
     * @param action the action
     */
    public void setUpdateAction(int action) throws SQLException {
        if (action == updateAction && updateSQL == null) {
            return;
        }
        if (updateAction != RESTRICT) {
            throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON UPDATE");
        }
        this.updateAction = action;
        buildUpdateSQL();
    }

    private void buildUpdateSQL() {
        if (updateAction == RESTRICT) {
            return;
        }
        StatementBuilder buff = new StatementBuilder();
        appendUpdate(buff);
        appendWhere(buff);
        updateSQL = buff.toString();
    }

    public void rebuild() {
        buildUpdateSQL();
        buildDeleteSQL();
    }

    private Prepared prepare(Session session, String sql, int action) throws SQLException {
        Prepared command = session.prepare(sql);
        if (action != CASCADE) {
            ObjectArray<Parameter> params = command.getParameters();
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i].column;
                Parameter param = params.get(i);
                Value value;
                if (action == SET_NULL) {
                    value = ValueNull.INSTANCE;
                } else {
                    Expression expr = column.getDefaultExpression();
                    if (expr == null) {
                        throw Message.getSQLException(ErrorCode.NO_DEFAULT_SET_1, column.getName());
                    }
                    value = expr.getValue(session);
                }
                param.setValue(value);
            }
        }
        return command;
    }

    private void appendUpdate(StatementBuilder buff) {
        buff.append("UPDATE ").append(table.getSQL()).append(" SET ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" , ");
            buff.append(Parser.quoteIdentifier(c.column.getName())).append("=?");
        }
    }

    private void appendWhere(StatementBuilder buff) {
        buff.append(" WHERE ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append(Parser.quoteIdentifier(c.column.getName())).append("=?");
        }
    }

    public Table getRefTable() {
        return refTable;
    }

    public boolean usesIndex(Index idx) {
        return idx == index || idx == refIndex;
    }

    public void setIndexOwner(Index index) {
        if (this.index == index) {
            indexOwner = true;
        } else if (this.refIndex == index) {
            refIndexOwner = true;
        } else {
            Message.throwInternalError();
        }
    }

    public boolean containsColumn(Column col) {
        for (IndexColumn c : columns) {
            if (c.column == col) {
                return true;
            }
        }
        for (IndexColumn c : refColumns) {
            if (c.column == col) {
                return true;
            }
        }
        return false;
    }

    public boolean isBefore() {
        return false;
    }

    public void checkExistingData(Session session) throws SQLException {
        if (session.getDatabase().isStarting()) {
            // don't check at startup
            return;
        }
        StatementBuilder buff = new StatementBuilder("SELECT 1 FROM (SELECT ");
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(" FROM ").append(table.getSQL()).append(" WHERE ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append(c.getSQL()).append(" IS NOT NULL ");
        }
        buff.append(" ORDER BY ");
        buff.resetCount();
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(") C WHERE NOT EXISTS(SELECT 1 FROM ").
            append(refTable.getSQL()).append(" P WHERE ");
        buff.resetCount();
        int i = 0;
        for (IndexColumn c : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append("C.").append(c.getSQL()).append('=').
                append("P.").append(refColumns[i++].getSQL());
        }
        buff.append(')');
        String sql = buff.toString();
        LocalResult r = session.prepare(sql).query(1);
        if (r.next()) {
            throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1,
                    getShortDescription());
        }
    }

    public Index getUniqueIndex() {
        return refIndex;
    }

}
