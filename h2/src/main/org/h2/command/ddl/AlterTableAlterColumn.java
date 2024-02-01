/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.command.CommandContainer;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.ParserBase;
import org.h2.command.Prepared;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.table.TableView;
import org.h2.util.HasSQL;
import org.h2.util.Utils;

/**
 * This class represents the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN DROP DEFAULT,
 * ALTER TABLE ALTER COLUMN DROP EXPRESSION,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE ALTER COLUMN DROP NULL,
 * ALTER TABLE ALTER COLUMN SET VISIBLE,
 * ALTER TABLE ALTER COLUMN SET INVISIBLE,
 * ALTER TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends CommandWithColumns {

    private String tableName;
    private Column oldColumn;
    private Column newColumn;
    private int type;
    /**
     * Default or on update expression.
     */
    private Expression defaultExpression;
    private Expression newSelectivity;
    private Expression usingExpression;
    private boolean addFirst;
    private String addBefore;
    private String addAfter;
    private boolean ifTableExists;
    private boolean ifNotExists;
    private ArrayList<Column> columnsToAdd;
    private ArrayList<Column> columnsToRemove;
    private boolean booleanFlag;

    public AlterTableAlterColumn(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setIfTableExists(boolean b) {
        ifTableExists = b;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setOldColumn(Column oldColumn) {
        this.oldColumn = oldColumn;
    }

    /**
     * Add the column as the first column of the table.
     */
    public void setAddFirst() {
        addFirst = true;
    }

    public void setAddBefore(String before) {
        this.addBefore = before;
    }

    public void setAddAfter(String after) {
        this.addAfter = after;
    }

    @Override
    public long update() {
        Database db = getDatabase();
        Table table = getSchema().resolveTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        session.getUser().checkTableRight(table, Right.SCHEMA_OWNER);
        table.checkSupportAlter();
        table.lock(session, Table.EXCLUSIVE_LOCK);
        if (newColumn != null) {
            checkDefaultReferencesTable(table, newColumn.getDefaultExpression());
            checkClustering(newColumn);
        }
        if (columnsToAdd != null) {
            for (Column column : columnsToAdd) {
                checkDefaultReferencesTable(table, column.getDefaultExpression());
                checkClustering(column);
            }
        }
        switch (type) {
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
            if (oldColumn == null || !oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNoNullValues(table);
            oldColumn.setNullable(false);
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_NOT_NULL: {
            if (oldColumn == null || oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNullable(table);
            oldColumn.setNullable(true);
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_EXPRESSION:  {
            if (oldColumn == null) {
                break;
            }
            if (oldColumn.isIdentity()) {
                break;
            }
            if (defaultExpression != null) {
                if (oldColumn.isGenerated()) {
                    break;
                }
                checkDefaultReferencesTable(table, defaultExpression);
                oldColumn.setDefaultExpression(session, defaultExpression);
            } else {
                if (type == CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_EXPRESSION != oldColumn.isGenerated()) {
                    break;
                }
                oldColumn.setDefaultExpression(session, null);
            }
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DROP_IDENTITY:  {
            if (oldColumn == null) {
                break;
            }
            Sequence sequence = oldColumn.getSequence();
            if (sequence == null) {
                break;
            }
            oldColumn.setSequence(null, false);
            removeSequence(table, sequence);
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_ON_UPDATE: {
            if (oldColumn == null) {
                break;
            }
            if (defaultExpression != null) {
                if (oldColumn.isIdentity() || oldColumn.isGenerated()) {
                    break;
                }
                checkDefaultReferencesTable(table, defaultExpression);
                oldColumn.setOnUpdateExpression(session, defaultExpression);
            } else {
                oldColumn.setOnUpdateExpression(session, null);
            }
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: {
            if (oldColumn == null) {
                break;
            }
            // if the change is only increasing the precision, then we don't
            // need to copy the table because the length is only a constraint,
            // and does not affect the storage structure.
            if (oldColumn.isWideningConversion(newColumn) && usingExpression == null) {
                convertIdentityColumn(table, oldColumn, newColumn);
                oldColumn.copy(newColumn);
                db.updateMeta(session, table);
            } else {
                oldColumn.setSequence(null, false);
                oldColumn.setDefaultExpression(session, null);
                if (oldColumn.isNullable() && !newColumn.isNullable()) {
                    checkNoNullValues(table);
                } else if (!oldColumn.isNullable() && newColumn.isNullable()) {
                    checkNullable(table);
                }
                if (oldColumn.getVisible() ^ newColumn.getVisible()) {
                    oldColumn.setVisible(newColumn.getVisible());
                }
                convertIdentityColumn(table, oldColumn, newColumn);
                copyData(table, null, true);
            }
            table.setModified();
            break;
        }
        case CommandInterface.ALTER_TABLE_ADD_COLUMN: {
            // ifNotExists only supported for single column add
            if (ifNotExists && columnsToAdd != null && columnsToAdd.size() == 1 &&
                    table.doesColumnExist(columnsToAdd.get(0).getName())) {
                break;
            }
            ArrayList<Sequence> sequences = generateSequences(columnsToAdd, false);
            if (columnsToAdd != null) {
                changePrimaryKeysToNotNull(columnsToAdd);
            }
            copyData(table, sequences, true);
            break;
        }
        case CommandInterface.ALTER_TABLE_DROP_COLUMN: {
            if (table.getColumns().length - columnsToRemove.size() < 1) {
                throw DbException.get(ErrorCode.CANNOT_DROP_LAST_COLUMN, columnsToRemove.get(0).getTraceSQL());
            }
            table.dropMultipleColumnsConstraintsAndIndexes(session, columnsToRemove);
            copyData(table, null, false);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY: {
            if (oldColumn == null) {
                break;
            }
            int value = newSelectivity.optimize(session).getValue(session).getInt();
            oldColumn.setSelectivity(value);
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_VISIBILITY:
            if (oldColumn == null) {
                break;
            }
            if (oldColumn.getVisible() != booleanFlag) {
                oldColumn.setVisible(booleanFlag);
                table.setModified();
                db.updateMeta(session, table);
            }
            break;
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT_ON_NULL:
            if (oldColumn == null) {
                break;
            }
            if (oldColumn.isDefaultOnNull() != booleanFlag) {
                oldColumn.setDefaultOnNull(booleanFlag);
                table.setModified();
                db.updateMeta(session, table);
            }
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return 0;
    }

    private static void checkDefaultReferencesTable(Table table, Expression defaultExpression) {
        if (defaultExpression == null) {
            return;
        }
        HashSet<DbObject> dependencies = new HashSet<>();
        ExpressionVisitor visitor = ExpressionVisitor
                .getDependenciesVisitor(dependencies);
        defaultExpression.isEverything(visitor);
        if (dependencies.contains(table)) {
            throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, defaultExpression.getTraceSQL());
        }
    }

    private void checkClustering(Column c) {
        if (!Constants.CLUSTERING_DISABLED
                .equals(getDatabase().getCluster())
                && c.hasIdentityOptions()) {
            throw DbException.getUnsupportedException(
                    "CLUSTERING && identity columns");
        }
    }

    private void convertIdentityColumn(Table table, Column oldColumn, Column newColumn) {
        if (newColumn.hasIdentityOptions()) {
            // Primary key creation is only needed for legacy
            // ALTER TABLE name ALTER COLUMN columnName IDENTITY
            if (newColumn.isPrimaryKey() && !oldColumn.isPrimaryKey()) {
                addConstraintCommand(
                        Parser.newPrimaryKeyConstraintCommand(session, table.getSchema(), table.getName(), newColumn));
            }
            int objId = getObjectId();
            newColumn.initializeSequence(session, getSchema(), objId, table.isTemporary());
        }
    }

    private void removeSequence(Table table, Sequence sequence) {
        if (sequence != null) {
            table.removeSequence(sequence);
            sequence.setBelongsToTable(false);
            Database db = getDatabase();
            db.removeSchemaObject(session, sequence);
        }
    }

    private void copyData(Table table, ArrayList<Sequence> sequences, boolean createConstraints) {
        if (table.isTemporary()) {
            throw DbException.getUnsupportedException("TEMP TABLE");
        }
        Database db = getDatabase();
        String baseName = table.getName();
        String tempName = db.getTempTableName(baseName, session);
        Column[] columns = table.getColumns();
        ArrayList<Column> newColumns = new ArrayList<>(columns.length);
        Table newTable = cloneTableStructure(table, columns, db, tempName, newColumns);
        if (sequences != null) {
            for (Sequence sequence : sequences) {
                table.addSequence(sequence);
            }
        }
        try {
            // check if a view would become invalid
            // (because the column to drop is referenced or so)
            checkViews(table, newTable);
        } catch (DbException e) {
            StringBuilder builder = new StringBuilder("DROP TABLE ");
            newTable.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS);
            execute(builder.toString());
            throw e;
        }
        String tableName = table.getName();
        ArrayList<TableView> dependentViews = new ArrayList<>(table.getDependentViews());
        for (TableView view : dependentViews) {
            table.removeDependentView(view);
        }
        StringBuilder builder = new StringBuilder("DROP TABLE ");
        table.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS).append(" IGNORE");
        execute(builder.toString());
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
        if (createConstraints) {
            createConstraints();
        }
        for (TableView view : dependentViews) {
            String sql = view.getCreateSQL(true, true);
            execute(sql);
        }
    }

    private Table cloneTableStructure(Table table, Column[] columns, Database db,
            String tempName, ArrayList<Column> newColumns) {
        for (Column col : columns) {
            newColumns.add(col.getClone());
        }
        switch (type) {
        case CommandInterface.ALTER_TABLE_DROP_COLUMN:
            for (Column removeCol : columnsToRemove) {
                Column foundCol = null;
                for (Column newCol : newColumns) {
                    if (newCol.getName().equals(removeCol.getName())) {
                        foundCol = newCol;
                        break;
                    }
                }
                if (foundCol == null) {
                    throw DbException.getInternalError(removeCol.getCreateSQL());
                }
                newColumns.remove(foundCol);
            }
            break;
        case CommandInterface.ALTER_TABLE_ADD_COLUMN: {
            int position;
            if (addFirst) {
                position = 0;
            } else if (addBefore != null) {
                position = table.getColumn(addBefore).getColumnId();
            } else if (addAfter != null) {
                position = table.getColumn(addAfter).getColumnId() + 1;
            } else {
                position = columns.length;
            }
            if (columnsToAdd != null) {
                for (Column column : columnsToAdd) {
                    newColumns.add(position++, column);
                }
            }
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE:
            newColumns.set(oldColumn.getColumnId(), newColumn);
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
        data.session = session;
        Table newTable = getSchema().createTable(data);
        newTable.setComment(table.getComment());
        String newTableSQL = newTable.getCreateSQLForMeta();
        StringBuilder columnNames = new StringBuilder();
        StringBuilder columnValues = new StringBuilder();
        for (Column nc : newColumns) {
            if (nc.isGenerated()) {
                continue;
            }
            switch (type) {
            case CommandInterface.ALTER_TABLE_ADD_COLUMN:
                if (columnsToAdd != null && columnsToAdd.contains(nc)) {
                    if (usingExpression != null) {
                        usingExpression.getUnenclosedSQL(addColumn(nc, columnNames, columnValues),
                                HasSQL.DEFAULT_SQL_FLAGS);
                    }
                    continue;
                }
                break;
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE:
                if (nc.equals(newColumn) && usingExpression != null) {
                    usingExpression.getUnenclosedSQL(addColumn(nc, columnNames, columnValues),
                            HasSQL.DEFAULT_SQL_FLAGS);
                    continue;
                }
            }
            nc.getSQL(addColumn(nc, columnNames, columnValues), HasSQL.DEFAULT_SQL_FLAGS);
        }
        String newTableName = newTable.getName();
        Schema newTableSchema = newTable.getSchema();
        newTable.removeChildrenAndResources(session);

        execute(newTableSQL);
        newTable = newTableSchema.getTableOrView(session, newTableName);
        ArrayList<String> children = Utils.newSmallArrayList();
        ArrayList<String> triggers = Utils.newSmallArrayList();
        boolean hasDelegateIndex = false;
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
                throw DbException.getInternalError();
            }
            String quotedName = ParserBase.quoteIdentifier(tempName + "_" + child.getName(), HasSQL.DEFAULT_SQL_FLAGS);
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
                    if (!hasDelegateIndex) {
                        Index index = null;
                        if (child instanceof ConstraintUnique) {
                            ConstraintUnique constraint = (ConstraintUnique) child;
                            if (constraint.getConstraintType() == Constraint.Type.PRIMARY_KEY) {
                                index = constraint.getIndex();
                            }
                        } else if (child instanceof Index) {
                            index = (Index) child;
                        }
                        if (index != null
                                && TableBase.getMainIndexColumn(index.getIndexType(), index.getIndexColumns())
                                        != SearchRow.ROWID_INDEX) {
                            execute(sql);
                            hasDelegateIndex = true;
                            continue;
                        }
                    }
                    children.add(sql);
                }
            }
        }
        StringBuilder builder = newTable.getSQL(new StringBuilder(128).append("INSERT INTO "), //
                HasSQL.DEFAULT_SQL_FLAGS)
            .append('(').append(columnNames).append(") OVERRIDING SYSTEM VALUE SELECT ");
        if (columnValues.length() == 0) {
            // special case: insert into test select * from
            builder.append('*');
        } else {
            builder.append(columnValues);
        }
        table.getSQL(builder.append(" FROM "), HasSQL.DEFAULT_SQL_FLAGS);
        try {
            execute(builder.toString());
        } catch (Throwable t) {
            // data was not inserted due to data conversion error or some
            // unexpected reason
            builder = new StringBuilder("DROP TABLE ");
            newTable.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS);
            execute(builder.toString());
            throw t;
        }
        for (String sql : children) {
            execute(sql);
        }
        table.setModified();
        // remove the sequences from the columns (except dropped columns)
        // otherwise the sequence is dropped if the table is dropped
        for (Column col : newColumns) {
            Sequence seq = col.getSequence();
            if (seq != null) {
                table.removeSequence(seq);
                col.setSequence(null, false);
            }
        }
        for (String sql : triggers) {
            execute(sql);
        }
        return newTable;
    }

    private static StringBuilder addColumn(Column column, StringBuilder columnNames, StringBuilder columnValues) {
        if (columnNames.length() > 0) {
            columnNames.append(", ");
        }
        column.getSQL(columnNames, HasSQL.DEFAULT_SQL_FLAGS);
        if (columnValues.length() > 0) {
            columnValues.append(", ");
        }
        return columnValues;
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
                String sql = ((TableView) view).getQuerySQL();
                // check if the query is still valid
                // do not execute, not even with limit 1, because that could
                // have side effects or take a very long time
                try {
                    session.prepare(sql);
                } catch (DbException e) {
                    throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1, e, view.getTraceSQL());
                }
                checkViewsAreValid(view);
            }
        }
    }

    private void execute(String sql) {
        Prepared command = session.prepare(sql);
        CommandContainer commandContainer = new CommandContainer(session, sql, command);
        commandContainer.executeUpdate(null);
    }

    private void checkNullable(Table table) {
        if (oldColumn.isIdentity()) {
            throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, oldColumn.getName());
        }
        for (Index index : table.getIndexes()) {
            if (index.getColumnIndex(oldColumn) < 0) {
                continue;
            }
            IndexType indexType = index.getIndexType();
            if (indexType.isPrimaryKey()) {
                throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, oldColumn.getName());
            }
        }
    }

    private void checkNoNullValues(Table table) {
        StringBuilder builder = new StringBuilder("SELECT COUNT(*) FROM ");
        table.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS).append(" WHERE ");
        oldColumn.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS).append(" IS NULL");
        String sql = builder.toString();
        Prepared command = session.prepare(sql);
        ResultInterface result = command.query(0);
        result.next();
        if (result.currentRow()[0].getInt() > 0) {
            throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, oldColumn.getTraceSQL());
        }
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setSelectivity(Expression selectivity) {
        newSelectivity = selectivity;
    }

    /**
     * Set default or on update expression.
     *
     * @param defaultExpression default or on update expression
     */
    public void setDefaultExpression(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    /**
     * Set using expression.
     *
     * @param usingExpression using expression
     */
    public void setUsingExpression(Expression usingExpression) {
        this.usingExpression = usingExpression;
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

    @Override
    public void addColumn(Column column) {
        if (columnsToAdd == null) {
            columnsToAdd = new ArrayList<>();
        }
        columnsToAdd.add(column);
    }

    public void setColumnsToRemove(ArrayList<Column> columnsToRemove) {
        this.columnsToRemove = columnsToRemove;
    }

    public void setBooleanFlag(boolean booleanFlag) {
        this.booleanFlag = booleanFlag;
    }
}
