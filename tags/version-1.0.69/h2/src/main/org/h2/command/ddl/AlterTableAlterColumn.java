/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;

/**
 * This class represents the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends SchemaCommand {

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET NOT NULL statement.
     */
    public static final int NOT_NULL = 0;
    
    /**
     * The type of a ALTER TABLE ALTER COLUMN SET NULL statement.
     */
    public static final int NULL = 1;
    
    /**
     * The type of a ALTER TABLE ALTER COLUMN SET DEFAULT statement.
     */
    public static final int DEFAULT = 2;
    
    /**
     * The type of a ALTER TABLE ALTER COLUMN statement that changes the column
     * data type.
     */
    public static final int CHANGE_TYPE = 3;
    
    /**
     * The type of a ALTER TABLE ADD statement.
     */
    public static final int ADD = 4;
    
    /**
     * The type of a ALTER TABLE DROP COLUMN statement.
     */
    public static final int DROP = 5;
    
    /**
     * The type of a ALTER TABLE ALTER COLUMN SELECTIVITY statement.
     */
    public static final int SELECTIVITY = 6;

    private Table table;
    private Column oldColumn;
    private Column newColumn;
    private int type;
    private Expression defaultExpression;
    private Expression newSelectivity;
    private String addBefore;

    public AlterTableAlterColumn(Session session, Schema schema) {
        super(session, schema);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setOldColumn(Column oldColumn) {
        this.oldColumn = oldColumn;
    }

    public void setAddBefore(String before) {
        this.addBefore = before;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        table.lock(session, true, true);
        Sequence sequence = oldColumn == null ? null : oldColumn.getSequence();
        switch (type) {
        case NOT_NULL: {
            if (!oldColumn.getNullable()) {
                // no change
                break;
            }
            checkNoNullValues();
            oldColumn.setNullable(false);
            db.update(session, table);
            break;
        }
        case NULL: {
            if (oldColumn.getNullable()) {
                // no change
                break;
            }
            checkNullable();
            oldColumn.setNullable(true);
            db.update(session, table);
            break;
        }
        case DEFAULT: {
            oldColumn.setSequence(null);
            oldColumn.setDefaultExpression(session, defaultExpression);
            removeSequence(session, sequence);
            db.update(session, table);
            break;
        }
        case CHANGE_TYPE: {
            // TODO document data type change problems when used with
            // autoincrement columns.
            // sequence will be unlinked
            checkNoViews();
            oldColumn.setSequence(null);
            oldColumn.setDefaultExpression(session, null);
            oldColumn.setConvertNullToDefault(false);
            if (oldColumn.getNullable() && !newColumn.getNullable()) {
                checkNoNullValues();
            } else if (!oldColumn.getNullable() && newColumn.getNullable()) {
                checkNullable();
            }
            convertToIdentityIfRequired(newColumn);
            copyData();
            break;
        }
        case ADD: {
            checkNoViews();
            convertToIdentityIfRequired(newColumn);
            copyData();
            break;
        }
        case DROP: {
            checkNoViews();
            if (table.getColumns().length == 1) {
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_LAST_COLUMN, oldColumn.getSQL());
            }
            table.checkColumnIsNotReferenced(oldColumn);
            dropSingleColumnIndexes();
            copyData();
            break;
        }
        case SELECTIVITY: {
            int value = newSelectivity.optimize(session).getValue(session).getInt();
            oldColumn.setSelectivity(value);
            db.update(session, table);
            break;
        }
        default:
            throw Message.getInternalError("type=" + type);
        }
        return 0;
    }

    private void convertToIdentityIfRequired(Column c) throws SQLException {
        if (c.getAutoIncrement()) {
            c.setOriginalSQL("IDENTITY");
        }
    }

    private void removeSequence(Session session, Sequence sequence) throws SQLException {
        if (sequence != null) {
            table.removeSequence(session, sequence);
            sequence.setBelongsToTable(false);
            Database db = session.getDatabase();
            db.removeSchemaObject(session, sequence);
        }
    }

    private void checkNoViews() throws SQLException {
        ObjectArray children = table.getChildren();
        for (int i = 0; i < children.size(); i++) {
            DbObject child = (DbObject) children.get(i);
            if (child.getType() == DbObject.TABLE_OR_VIEW) {
                throw Message.getSQLException(ErrorCode.OPERATION_NOT_SUPPORTED_WITH_VIEWS_2, new String[] {
                        table.getName(), child.getName() });
            }
        }
    }

    private void copyData() throws SQLException {
        Database db = session.getDatabase();
        String tempName = db.getTempTableName(session.getId());
        Column[] columns = table.getColumns();
        ObjectArray newColumns = new ObjectArray();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i].getClone();
            newColumns.add(col);
        }
        if (type == DROP) {
            int position = oldColumn.getColumnId();
            newColumns.remove(position);
        } else if (type == ADD) {
            int position;
            if (addBefore == null) {
                position = columns.length;
            } else {
                position = table.getColumn(addBefore).getColumnId();
            }
            newColumns.add(position, newColumn);
        } else if (type == CHANGE_TYPE) {
            int position = oldColumn.getColumnId();
            newColumns.remove(position);
            newColumns.add(position, newColumn);
        }
        boolean persistent = table.isPersistent();
        // create a table object in order to get the SQL statement
        // can't just use this table, because most column objects are 'shared'
        // with the old table
        // still need a new id because using 0 would mean: the new table tries
        // to use the rows of the table 0 (the script table)
        int id = -1;
        TableData newTable = getSchema().createTable(tempName, id, newColumns, persistent, false);
        newTable.setComment(table.getComment());
        execute(newTable.getCreateSQL(), true);
        newTable = (TableData) newTable.getSchema().getTableOrView(session, newTable.getName());
        ObjectArray children = table.getChildren();
        for (int i = 0; i < children.size(); i++) {
            DbObject child = (DbObject) children.get(i);
            if (child instanceof Sequence) {
                continue;
            } else if (child instanceof Index) {
                Index idx = (Index) child;
                if (idx.getIndexType().belongsToConstraint()) {
                    continue;
                }
            }
            String createSQL = child.getCreateSQL();
            if (createSQL == null) {
                continue;
            }
            if (child.getType() == DbObject.TABLE_OR_VIEW) {
                throw Message.getInternalError();
            }
            String quotedName = Parser.quoteIdentifier(tempName + "_" + child.getName());
            String sql = null;
            if (child instanceof ConstraintReferential) {
                ConstraintReferential r = (ConstraintReferential) child;
                if (r.getTable() != table) {
                    sql = r.getCreateSQLForCopy(r.getTable(), newTable, quotedName, false);
                }
            }
            if (sql == null) {
                sql = child.getCreateSQLForCopy(newTable, quotedName);
            }
            if (sql != null) {
                execute(sql, true);
            }
        }
        StringBuffer columnList = new StringBuffer();
        for (int i = 0; i < newColumns.size(); i++) {
            Column nc = (Column) newColumns.get(i);
            if (type == ADD && nc == newColumn) {
                continue;
            }
            if (columnList.length() > 0) {
                columnList.append(", ");
            }
            columnList.append(nc.getSQL());
        }
        // TODO loop instead of use insert (saves memory)
        /*
         *
         * Index scan = table.getBestPlanItem(null).getIndex(); Cursor cursor =
         * scan.find(null, null); while (cursor.next()) { Row row =
         * cursor.get(); Row newRow = newTable.getTemplateRow(); for (int i=0,
         * j=0; i<columns.length; i++) { if(i == position) { continue; }
         * newRow.setValue(j++, row.getValue(i)); }
         * newTable.validateAndConvert(newRow); newTable.addRow(newRow); }
         */
        StringBuffer buff = new StringBuffer();
        buff.append("INSERT INTO ");
        buff.append(newTable.getSQL());
        buff.append("(");
        buff.append(columnList);
        buff.append(") SELECT ");
        if (columnList.length() == 0) {
            // special case insert into test select * from test
            buff.append("*");
        } else {
            buff.append(columnList);
        }
        buff.append(" FROM ");
        buff.append(table.getSQL());
        String sql = buff.toString();
        newTable.setCheckForeignKeyConstraints(session, false, false);
        try {
            execute(sql, false);
        } catch (SQLException e) {
            unlinkSequences(newTable);
            execute("DROP TABLE " + newTable.getSQL(), true);
            throw e;
        }
        newTable.setCheckForeignKeyConstraints(session, true, false);
        String tableName = table.getName();
        table.setModified();
        for (int i = 0; i < columns.length; i++) {
            // if we don't do that, the sequence is dropped when the table is
            // dropped
            Sequence seq = columns[i].getSequence();
            if (seq != null) {
                table.removeSequence(session, seq);
                columns[i].setSequence(null);
            }
        }
        execute("DROP TABLE " + table.getSQL(), true);
        db.renameSchemaObject(session, newTable, tableName);
        children = newTable.getChildren();
        for (int i = 0; i < children.size(); i++) {
            DbObject child = (DbObject) children.get(i);
            if (child instanceof Sequence) {
                continue;
            }
            String name = child.getName();
            if (name == null || child.getCreateSQL() == null) {
                continue;
            }
            if (name.startsWith(tempName + "_")) {
                name = name.substring(tempName.length() + 1);
                db.renameSchemaObject(session, (SchemaObject) child, name);
            }
        }
    }

    private void unlinkSequences(Table table) throws SQLException {
        Column[] columns = table.getColumns();
        for (int i = 0; i < columns.length; i++) {
            // if we don't do that, the sequence is dropped when the table is
            // dropped
            Sequence seq = columns[i].getSequence();
            if (seq != null) {
                table.removeSequence(session, seq);
                columns[i].setSequence(null);
            }
        }
    }

    private void execute(String sql, boolean ddl) throws SQLException {
        Prepared command = session.prepare(sql);
        command.update();
        if (ddl && session.getDatabase().isMultiVersion()) {
            // TODO this should work without MVCC, but avoid risks at the moment
            session.commit(true);
        }
    }

    private void dropSingleColumnIndexes() throws SQLException {
        Database db = session.getDatabase();
        ObjectArray indexes = table.getIndexes();
        for (int i = 0; i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            if (index.getCreateSQL() == null) {
                continue;
            }
            boolean dropIndex = false;
            Column[] cols = index.getColumns();
            for (int j = 0; j < cols.length; j++) {
                if (cols[j] == oldColumn) {
                    if (cols.length == 1) {
                        dropIndex = true;
                    } else {
                        throw Message.getSQLException(ErrorCode.COLUMN_IS_PART_OF_INDEX_1, index.getSQL());
                    }
                }
            }
            if (dropIndex) {
                db.removeSchemaObject(session, index);
                indexes = table.getIndexes();
                i = -1;
            }
        }
    }

    private void checkNullable() throws SQLException {
        ObjectArray indexes = table.getIndexes();
        for (int i = 0; i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            if (index.getColumnIndex(oldColumn) < 0) {
                continue;
            }
            IndexType indexType = index.getIndexType();
            if (indexType.isPrimaryKey() || indexType.isHash()) {
                throw Message.getSQLException(ErrorCode.COLUMN_IS_PART_OF_INDEX_1, index.getSQL());
            }
        }
    }

    private void checkNoNullValues() throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + table.getSQL() + " WHERE " + oldColumn.getSQL() + " IS NULL";
        Prepared command = session.prepare(sql);
        LocalResult result = command.query(0);
        result.next();
        if (result.currentRow()[0].getInt() > 0) {
            throw Message.getSQLException(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, oldColumn.getSQL());
        }
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setSelectivity(Expression selectivity) {
        newSelectivity = selectivity;
    }

    public void setDefaultExpression(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    public void setNewColumn(Column newColumn) {
        this.newColumn = newColumn;
    }

}
