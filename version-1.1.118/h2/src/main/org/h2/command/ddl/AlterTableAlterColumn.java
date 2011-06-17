/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
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
import org.h2.schema.TriggerObject;
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
            if (!oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNoNullValues();
            oldColumn.setNullable(false);
            db.update(session, table);
            break;
        }
        case NULL: {
            if (oldColumn.isNullable()) {
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
            removeSequence(sequence);
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
            if (oldColumn.isNullable() && !newColumn.isNullable()) {
                checkNoNullValues();
            } else if (!oldColumn.isNullable() && newColumn.isNullable()) {
                checkNullable();
            }
            convertAutoIncrementColumn(newColumn);
            copyData();
            break;
        }
        case ADD: {
            checkNoViews();
            convertAutoIncrementColumn(newColumn);
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
            Message.throwInternalError("type=" + type);
        }
        return 0;
    }

    private void convertAutoIncrementColumn(Column c) throws SQLException {
        if (c.isAutoIncrement()) {
            if (c.isPrimaryKey()) {
                c.setOriginalSQL("IDENTITY");
            } else {
                int objId = getObjectId(true, true);
                c.convertAutoIncrementToSequence(session, getSchema(), objId, table.isTemporary());
            }
        }
    }

    private void removeSequence(Sequence sequence) throws SQLException {
        if (sequence != null) {
            table.removeSequence(session, sequence);
            sequence.setBelongsToTable(false);
            Database db = session.getDatabase();
            db.removeSchemaObject(session, sequence);
        }
    }

    private void checkNoViews() throws SQLException {
        for (DbObject child : table.getChildren()) {
            if (child.getType() == DbObject.TABLE_OR_VIEW) {
                throw Message.getSQLException(ErrorCode.OPERATION_NOT_SUPPORTED_WITH_VIEWS_2,
                        table.getName(), child.getName());
            }
        }
    }

    private void copyData() throws SQLException {
        Database db = session.getDatabase();
        String tempName = db.getTempTableName(session.getId());
        Column[] columns = table.getColumns();
        ObjectArray<Column> newColumns = ObjectArray.newInstance();
        for (Column col : columns) {
            newColumns.add(col.getClone());
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

        // create a table object in order to get the SQL statement
        // can't just use this table, because most column objects are 'shared'
        // with the old table
        // still need a new id because using 0 would mean: the new table tries
        // to use the rows of the table 0 (the meta table)
        int id = db.allocateObjectId(true, true);
        TableData newTable = getSchema().createTable(tempName, id, newColumns, table.isTemporary(), table.isPersistIndexes(), table.isPersistData(), false, Index.EMPTY_HEAD, session);
        newTable.setComment(table.getComment());
        StringBuilder buff = new StringBuilder();
        buff.append(newTable.getCreateSQL());
        StringBuilder columnList = new StringBuilder();
        for (Column nc : newColumns) {
            if (columnList.length() > 0) {
                columnList.append(", ");
            }
            if (type == ADD && nc == newColumn) {
                Expression def = nc.getDefaultExpression();
                columnList.append(def == null ? "NULL" : def.getSQL());
            } else {
                columnList.append(nc.getSQL());
            }
        }
        buff.append(" AS SELECT ");
        if (columnList.length() == 0) {
            // special case insert into test select * from test
            buff.append('*');
        } else {
            buff.append(columnList);
        }
        buff.append(" FROM ").append(table.getSQL());
        String newTableSQL = buff.toString();
        String newTableName = newTable.getName();
        Schema newTableSchema = newTable.getSchema();
        newTable.removeChildrenAndResources(session);

        execute(newTableSQL, true);
        newTable = (TableData) newTableSchema.getTableOrView(session, newTableName);
        ObjectArray<String> triggers = ObjectArray.newInstance();
        for (DbObject child : table.getChildren()) {
            if (child instanceof Sequence) {
                continue;
            } else if (child instanceof Index) {
                Index idx = (Index) child;
                if (idx.getIndexType().getBelongsToConstraint()) {
                    continue;
                }
            }
            String createSQL = child.getCreateSQL();
            if (createSQL == null) {
                continue;
            }
            if (child.getType() == DbObject.TABLE_OR_VIEW) {
                Message.throwInternalError();
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
                if (child instanceof TriggerObject) {
                    triggers.add(sql);
                } else {
                    execute(sql, true);
                }
            }
        }
        String tableName = table.getName();
        table.setModified();
        // remove the sequences from the columns (except dropped columns)
        // otherwise the sequence is dropped if the table is dropped
        for (Column col : newColumns) {
            Sequence seq = col.getSequence();
            if (seq != null) {
                table.removeSequence(session, seq);
                col.setSequence(null);
            }
        }
        for (String sql : triggers) {
            execute(sql, true);
        }
        execute("DROP TABLE " + table.getSQL(), true);
        db.renameSchemaObject(session, newTable, tableName);
        for (DbObject child : newTable.getChildren()) {
            if (child instanceof Sequence) {
                continue;
            }
            String name = child.getName();
            if (name == null || child.getCreateSQL() == null) {
                continue;
            }
            if (name.startsWith(tempName + "_")) {
                name = name.substring(tempName.length() + 1);
                SchemaObject so = (SchemaObject) child;
                if (so instanceof Constraint) {
                    if (so.getSchema().findConstraint(session, name) != null) {
                        name = so.getSchema().getUniqueConstraintName(session, newTable);
                    }
                } else if (so instanceof Index) {
                    if (so.getSchema().findIndex(session, name) != null) {
                        name = so.getSchema().getUniqueIndexName(session, newTable, name);
                    }
                }
                db.renameSchemaObject(session, so, name);
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
        ObjectArray<Index> indexes = table.getIndexes();
        for (int i = 0; i < indexes.size(); i++) {
            Index index = indexes.get(i);
            if (index.getCreateSQL() == null) {
                continue;
            }
            boolean dropIndex = false;
            Column[] cols = index.getColumns();
            for (Column c : cols) {
                if (c == oldColumn) {
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
        for (Index index : table.getIndexes()) {
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
