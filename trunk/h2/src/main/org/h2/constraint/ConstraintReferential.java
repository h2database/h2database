/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A referential constraint.
 */
public class ConstraintReferential extends Constraint {
    public static final int RESTRICT = 0, CASCADE = 1, SET_DEFAULT = 2, SET_NULL = 3;

    private int deleteAction;
    private int updateAction;
    private Table refTable;
    private Index index;
    private Index refIndex;
    private boolean indexOwner;
    private boolean refIndexOwner;
    protected Column[] columns;
    protected Column[] refColumns;
    private String deleteSQL, updateSQL;
    private boolean skipOwnTable;

    public ConstraintReferential(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    public String getConstraintType() {
        return Constraint.REFERENTIAL;
    }

    private void appendAction(StringBuffer buff, int action) {
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
            throw Message.getInternalError("action=" + action);
        }
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        return getCreateSQLForCopy(table, refTable, quotedName, true);
    }

    public String getCreateSQLForCopy(Table table, Table refTable, String quotedName, boolean internalIndex) {
        StringBuffer buff = new StringBuffer();
        buff.append("ALTER TABLE ");
        String mainTable = table.getSQL();
        buff.append(mainTable);
        buff.append(" ADD CONSTRAINT ");
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        Column[] cols = columns;
        Column[] refCols = refColumns;
        buff.append(" FOREIGN KEY(");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(cols[i].getSQL());
        }
        buff.append(")");
        if (internalIndex && indexOwner && table == this.table) {
            buff.append(" INDEX ");
            buff.append(index.getSQL());
        }
        buff.append(" REFERENCES ");
        String quotedRefTable;
        if (this.table == this.refTable) {
            // self-referencing constraints: need to use new table
            quotedRefTable = table.getSQL();
        } else {
            quotedRefTable = refTable.getSQL();
        }
        buff.append(quotedRefTable);
        buff.append("(");
        for (int i = 0; i < refCols.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(refCols[i].getSQL());
        }
        buff.append(")");
        if (internalIndex && refIndexOwner && table == this.table) {
            buff.append(" INDEX ");
            buff.append(refIndex.getSQL());
        }
        if (deleteAction != RESTRICT) {
            buff.append(" ON DELETE ");
            appendAction(buff, deleteAction);
        }
        if (updateAction != RESTRICT) {
            buff.append(" ON UPDATE ");
            appendAction(buff, updateAction);
        }
        buff.append(" NOCHECK");
        return buff.toString();
    }

    public String getShortDescription() {
        StringBuffer buff = new StringBuffer();
        buff.append(getName());
        buff.append(": ");
        buff.append(table.getSQL());
        buff.append(" FOREIGN KEY(");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(")");
        buff.append(" REFERENCES ");
        buff.append(refTable.getSQL());
        buff.append("(");
        for (int i = 0; i < refColumns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(refColumns[i].getSQL());
        }
        buff.append(")");
        return buff.toString();
    }

    public String getCreateSQLWithoutIndexes() {
        return getCreateSQLForCopy(table, refTable, getSQL(), false);
    }

    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    public void setColumns(Column[] cols) {
        columns = cols;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setRefColumns(Column[] refCols) {
        refColumns = refCols;
    }

    public Column[] getRefColumns() {
        return refColumns;
    }

    public void setRefTable(Table refTable) {
        this.refTable = refTable;
        if (refTable.getTemporary()) {
            setTemporary(true);
        }
    }

    public void setIndex(Index index, boolean isOwner) {
        this.index = index;
        this.indexOwner = isOwner;
    }

    public void setRefIndex(Index refIndex, boolean isRefOwner) {
        this.refIndex = refIndex;
        this.refIndexOwner = isRefOwner;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        table.removeConstraint(this);
        refTable.removeConstraint(this);
        if (indexOwner) {
            database.removeSchemaObject(session, index);
        }
        if (refIndexOwner) {
            database.removeSchemaObject(session, refIndex);
        }
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
                checkRowOwnTable(session, newRow);
            }
        }
        if (t == refTable) {
            checkRowRefTable(session, oldRow, newRow);
        }
    }

    private void checkRowOwnTable(Session session, Row newRow) throws SQLException {
        if (newRow == null) {
            return;
        }
        boolean containsNull = false;
        for (int i = 0; i < columns.length; i++) {
            int idx = columns[i].getColumnId();
            Value v = newRow.getValue(idx);
            if (v == ValueNull.INSTANCE) {
                containsNull = true;
                break;
            }
        }
        if (containsNull) {
            return;
        }
        if (refTable == table) {
            // special case self referencing constraints: check the inserted row
            // first
            boolean self = true;
            for (int i = 0; i < columns.length; i++) {
                int idx = columns[i].getColumnId();
                Value v = newRow.getValue(idx);
                Column refCol = refColumns[i];
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
            int idx = columns[i].getColumnId();
            Value v = newRow.getValue(idx);
            Column refCol = refColumns[i];
            int refIdx = refCol.getColumnId();
            check.setValue(refIdx, v.convertTo(refCol.getType()));
        }
        if (!found(session, refIndex, check)) {
            throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1,
                    getShortDescription());
        }
    }

    private boolean found(Session session, Index index, SearchRow check) throws SQLException {
        index.getTable().lock(session, false, false);
        Cursor cursor = index.find(session, check, check);
        while (cursor.next()) {
            SearchRow found;
            found = cursor.getSearchRow();
            Column[] cols = index.getColumns();
            boolean allEqual = true;
            for (int i = 0; i < columns.length && i < cols.length; i++) {
                int idx = cols[i].getColumnId();
                Value c = check.getValue(idx);
                Value f = found.getValue(idx);
                if (database.compareTypeSave(c, f) != 0) {
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
        if (refTable == table) {
            // special case self referencing constraints: check the deleted row
            // first
            boolean self = true;
            for (int i = 0; i < columns.length; i++) {
                Column refCol = refColumns[i];
                int refIdx = refCol.getColumnId();
                Value v = oldRow.getValue(refIdx);
                int idx = columns[i].getColumnId();
                Value r = oldRow.getValue(idx);
                if (!database.areEqual(r, v)) {
                    self = false;
                    break;
                }
            }
            if (self) {
                return;
            }
        }
        SearchRow check = table.getTemplateSimpleRow(false);
        for (int i = 0; i < columns.length; i++) {
            Column refCol = refColumns[i];
            int refIdx = refCol.getColumnId();
            Column col = columns[i];
            int idx = col.getColumnId();
            Value v = oldRow.getValue(refIdx).convertTo(col.getType());
            check.setValue(idx, v);
        }
        if (found(session, index, check)) {
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
                    ObjectArray params = updateCommand.getParameters();
                    for (int i = 0; i < columns.length; i++) {
                        Parameter param = (Parameter) params.get(i);
                        Column refCol = refColumns[i];
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

    void setWhere(Prepared command, int pos, Row row) {
        for (int i = 0; i < refColumns.length; i++) {
            int idx = refColumns[i].getColumnId();
            Value v = row.getValue(idx);
            ObjectArray params = command.getParameters();
            Parameter param = (Parameter) params.get(pos + i);
            param.setValue(v);
        }
    }

    public int getDeleteAction() {
        return deleteAction;
    }

    public void setDeleteAction(Session session, int action) throws SQLException {
        if (action == deleteAction) {
            return;
        }
        if (deleteAction != RESTRICT) {
            throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON DELETE");
        }
        this.deleteAction = action;
        StringBuffer buff = new StringBuffer();
        if (action == CASCADE) {
            buff.append("DELETE FROM ");
            buff.append(table.getSQL());
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

    public void setUpdateAction(Session session, int action) throws SQLException {
        if (action == updateAction) {
            return;
        }
        if (updateAction != RESTRICT) {
            throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON UPDATE");
        }
        this.updateAction = action;
        StringBuffer buff = new StringBuffer();
        appendUpdate(buff);
        appendWhere(buff);
        updateSQL = buff.toString();
    }

    private Prepared prepare(Session session, String sql, int action) throws SQLException {
        Prepared command = session.prepare(sql);
        if (action != CASCADE) {
            ObjectArray params = command.getParameters();
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                Parameter param = (Parameter) params.get(i);
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

    private void appendUpdate(StringBuffer buff) {
        buff.append("UPDATE ");
        buff.append(table.getSQL());
        buff.append(" SET ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" , ");
            }
            Column column = columns[i];
            buff.append(Parser.quoteIdentifier(column.getName()));
            buff.append("=?");
        }
    }

    private void appendWhere(StringBuffer buff) {
        buff.append(" WHERE ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            Column column = columns[i];
            buff.append(Parser.quoteIdentifier(column.getName()));
            buff.append("=?");
        }
    }

    public Table getRefTable() {
        return refTable;
    }

    public boolean usesIndex(Index idx) {
        return idx == index || idx == refIndex;
    }

    public boolean containsColumn(Column col) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] == col) {
                return true;
            }
        }
        for (int i = 0; i < refColumns.length; i++) {
            if (refColumns[i] == col) {
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
        StringBuffer buff = new StringBuffer();
        buff.append("SELECT 1 FROM (SELECT ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(" FROM ");
        buff.append(table.getSQL());
        buff.append(" WHERE ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append(columns[i].getSQL());
            buff.append(" IS NOT NULL ");
        }
        buff.append(" ORDER BY ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(") C WHERE NOT EXISTS(SELECT 1 FROM ");
        buff.append(refTable.getSQL());
        buff.append(" P WHERE ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append("C.");
            buff.append(columns[i].getSQL());
            buff.append("=");
            buff.append("P.");
            buff.append(refColumns[i].getSQL());
        }
        buff.append(")");
        String sql = buff.toString();
        LocalResult r = session.prepare(sql).query(1);
        if (r.next()) {
            throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1,
                    getShortDescription());
        }
    }

}
