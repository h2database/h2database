/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableView;
import org.h2.util.New;

/**
 * This class represents the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends SchemaCommand {

    private Table table;
    private Column oldColumn;
    private Column newColumn;
    private int type;
    private Expression defaultExpression;
    private Expression newSelectivity;
    private String addBefore;
    private String addAfter;
    private boolean ifNotExists;
    private ArrayList<Column> columnsToAdd;

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

    public void setAddAfter(String after) {
        this.addAfter = after;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        table.lock(session, true, true);
        Sequence sequence = oldColumn == null ? null : oldColumn.getSequence();
        if (newColumn != null) {
            checkDefaultReferencesTable(newColumn.getDefaultExpression());
        }
        if (columnsToAdd != null) {
            for (Column column : columnsToAdd) {
                checkDefaultReferencesTable(column.getDefaultExpression());
            }
        }
        switch (type) {
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
            if (!oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNoNullValues();
            oldColumn.setNullable(false);
            db.update(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL: {
            if (oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNullable();
            oldColumn.setNullable(true);
            db.update(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT: {
            checkDefaultReferencesTable(defaultExpression);
            oldColumn.setSequence(null);
            oldColumn.setDefaultExpression(session, defaultExpression);
            removeSequence(sequence);
            db.update(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: {
            // if the change is only increasing the precision, then we don't
            // need to copy the table because the length is only a constraint,
            // and does not affect the storage structure.
            if (oldColumn.isWideningConversion(newColumn)) {
                convertAutoIncrementColumn(newColumn);
                oldColumn.copy(newColumn);
                db.update(session, table);
            } else {
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
            }
            break;
        }
        case CommandInterface.ALTER_TABLE_ADD_COLUMN: {
            // ifNotExists only supported for single column add
            if (ifNotExists && columnsToAdd.size() == 1 &&
                    table.doesColumnExist(columnsToAdd.get(0).getName())) {
                break;
            }
            for (Column column : columnsToAdd) {
                convertAutoIncrementColumn(column);
            }
            copyData();
            break;
        }
        case CommandInterface.ALTER_TABLE_DROP_COLUMN: {
            if (table.getColumns().length == 1) {
                throw DbException.get(ErrorCode.CANNOT_DROP_LAST_COLUMN,
                        oldColumn.getSQL());
            }
            table.dropSingleColumnConstraintsAndIndexes(session, oldColumn);
            copyData();
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY: {
            int value = newSelectivity.optimize(session).getValue(session).getInt();
            oldColumn.setSelectivity(value);
            db.update(session, table);
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    private void checkDefaultReferencesTable(Expression defaultExpression) {
        if (defaultExpression == null) {
            return;
        }
        HashSet<DbObject> dependencies = New.hashSet();
        ExpressionVisitor visitor = ExpressionVisitor
                .getDependenciesVisitor(dependencies);
        defaultExpression.isEverything(visitor);
        if (dependencies.contains(table)) {
            throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1,
                    defaultExpression.getSQL());
        }
    }

    private void convertAutoIncrementColumn(Column c) {
        if (c.isAutoIncrement()) {
            if (c.isPrimaryKey()) {
                c.setOriginalSQL("IDENTITY");
            } else {
                int objId = getObjectId();
                c.convertAutoIncrementToSequence(session, getSchema(), objId,
                        table.isTemporary());
            }
        }
    }

    private void removeSequence(Sequence sequence) {
        if (sequence != null) {
            table.removeSequence(sequence);
            sequence.setBelongsToTable(false);
            Database db = session.getDatabase();
            db.removeSchemaObject(session, sequence);
        }
    }

    private void copyData() {
        if (table.isTemporary()) {
            throw DbException.getUnsupportedException("TEMP TABLE");
        }
        Database db = session.getDatabase();
        String baseName = table.getName();
        String tempName = db.getTempTableName(baseName, session);
        Column[] columns = table.getColumns();
        ArrayList<Column> newColumns = New.arrayList();
        Table newTable = cloneTableStructure(columns, db, tempName, newColumns);
        try {
            // check if a view would become invalid
            // (because the column to drop is referenced or so)
            checkViews(table, newTable);
        } catch (DbException e) {
            execute("DROP TABLE " + newTable.getName(), true);
            throw DbException.get(ErrorCode.VIEW_IS_INVALID_2, e, getSQL(), e.getMessage());
        }
        String tableName = table.getName();
        ArrayList<TableView> views = table.getViews();
        if (views != null) {
            views = New.arrayList(views);
            for (TableView view : views) {
                table.removeView(view);
            }
        }
        execute("DROP TABLE " + table.getSQL() + " IGNORE", true);
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
        if (views != null) {
            for (TableView view : views) {
                String sql = view.getCreateSQL(true, true);
                execute(sql, true);
            }
        }
    }

    private Table cloneTableStructure(Column[] columns, Database db,
            String tempName, ArrayList<Column> newColumns) {
        for (Column col : columns) {
            newColumns.add(col.getClone());
        }
        if (type == CommandInterface.ALTER_TABLE_DROP_COLUMN) {
            int position = oldColumn.getColumnId();
            newColumns.remove(position);
        } else if (type == CommandInterface.ALTER_TABLE_ADD_COLUMN) {
            int position;
            if (addBefore != null) {
                position = table.getColumn(addBefore).getColumnId();
            } else if (addAfter != null) {
                position = table.getColumn(addAfter).getColumnId() + 1;
            } else {
                position = columns.length;
            }
            for (Column column : columnsToAdd) {
                newColumns.add(position++, column);
            }
        } else if (type == CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE) {
            int position = oldColumn.getColumnId();
            newColumns.remove(position);
            newColumns.add(position, newColumn);
        }

        // create a table object in order to get the SQL statement
        // can't just use this table, because most column objects are 'shared'
        // with the old table
        // still need a new id because using 0 would mean: the new table tries
        // to use the rows of the table 0 (the meta table)
        int id = db.allocateObjectId();
        CreateTableData data = new CreateTableData();
        data.tableName = tempName;
        data.id = id;
        data.columns = newColumns;
        data.temporary = table.isTemporary();
        data.persistData = table.isPersistData();
        data.persistIndexes = table.isPersistIndexes();
        data.isHidden = table.isHidden();
        data.create = true;
        data.session = session;
        Table newTable = getSchema().createTable(data);
        newTable.setComment(table.getComment());
        StringBuilder buff = new StringBuilder();
        buff.append(newTable.getCreateSQL());
        StringBuilder columnList = new StringBuilder();
        for (Column nc : newColumns) {
            if (columnList.length() > 0) {
                columnList.append(", ");
            }
            if (type == CommandInterface.ALTER_TABLE_ADD_COLUMN &&
                    columnsToAdd.contains(nc)) {
                Expression def = nc.getDefaultExpression();
                columnList.append(def == null ? "NULL" : def.getSQL());
            } else {
                columnList.append(nc.getSQL());
            }
        }
        buff.append(" AS SELECT ");
        if (columnList.length() == 0) {
            // special case: insert into test select * from
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
        newTable = newTableSchema.getTableOrView(session, newTableName);
        ArrayList<String> triggers = New.arrayList();
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
            if (child instanceof TableView) {
                continue;
            } else if (child.getType() == DbObject.TABLE_OR_VIEW) {
                DbException.throwInternalError();
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
        table.setModified();
        // remove the sequences from the columns (except dropped columns)
        // otherwise the sequence is dropped if the table is dropped
        for (Column col : newColumns) {
            Sequence seq = col.getSequence();
            if (seq != null) {
                table.removeSequence(seq);
                col.setSequence(null);
            }
        }
        for (String sql : triggers) {
            execute(sql, true);
        }
        return newTable;
    }

    /**
     * Check that all views and other dependent objects.
     */
    private void checkViews(SchemaObject sourceTable, SchemaObject newTable) {
        String sourceTableName = sourceTable.getName();
        String newTableName = newTable.getName();
        Database db = sourceTable.getDatabase();
        // save the real table under a temporary name
        String temp = db.getTempTableName(sourceTableName, session);
        db.renameSchemaObject(session, sourceTable, temp);
        try {
            // have our new table impersonate the target table
            db.renameSchemaObject(session, newTable, sourceTableName);
            checkViewsAreValid(sourceTable);
        } finally {
            // always put the source tables back with their proper names
            try {
                db.renameSchemaObject(session, newTable, newTableName);
            } finally {
                db.renameSchemaObject(session, sourceTable, sourceTableName);
            }
        }
    }

    /**
     * Check that a table or view is still valid.
     *
     * @param tableOrView the table or view to check
     */
    private void checkViewsAreValid(DbObject tableOrView) {
        for (DbObject view : tableOrView.getChildren()) {
            if (view instanceof TableView) {
                String sql = ((TableView) view).getQuery();
                // check if the query is still valid
                // do not execute, not even with limit 1, because that could
                // have side effects or take a very long time
                session.prepare(sql);
                checkViewsAreValid(view);
            }
        }
    }

    private void execute(String sql, boolean ddl) {
        Prepared command = session.prepare(sql);
        command.update();
        if (ddl) {
            session.commit(true);
        }
    }

    private void checkNullable() {
        for (Index index : table.getIndexes()) {
            if (index.getColumnIndex(oldColumn) < 0) {
                continue;
            }
            IndexType indexType = index.getIndexType();
            if (indexType.isPrimaryKey() || indexType.isHash()) {
                throw DbException.get(
                        ErrorCode.COLUMN_IS_PART_OF_INDEX_1, index.getSQL());
            }
        }
    }

    private void checkNoNullValues() {
        String sql = "SELECT COUNT(*) FROM " +
                table.getSQL() + " WHERE " +
                oldColumn.getSQL() + " IS NULL";
        Prepared command = session.prepare(sql);
        ResultInterface result = command.query(0);
        result.next();
        if (result.currentRow()[0].getInt() > 0) {
            throw DbException.get(
                    ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1,
                    oldColumn.getSQL());
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

    @Override
    public int getType() {
        return type;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setNewColumns(ArrayList<Column> columnsToAdd) {
        this.columnsToAdd = columnsToAdd;
    }
}
