/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.engine.NullsDistinct;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.HasSQL;
import org.h2.value.DataType;

/**
 * This class represents the statement
 * ALTER TABLE ADD CONSTRAINT
 */
public class AlterTableAddConstraint extends AlterTable {

    private final int type;
    private String constraintName;
    private NullsDistinct nullsDistinct;
    private IndexColumn[] indexColumns;
    private ConstraintActionType deleteAction = ConstraintActionType.RESTRICT;
    private ConstraintActionType updateAction = ConstraintActionType.RESTRICT;
    private Schema refSchema;
    private String refTableName;
    private IndexColumn[] refIndexColumns;
    private Expression checkExpression;
    private Index index, refIndex;
    private String comment;
    private boolean checkExisting;
    private boolean primaryKeyHash;
    private final boolean ifNotExists;
    private final ArrayList<Index> createdIndexes = new ArrayList<>();
    private ConstraintUnique createdUniqueConstraint;

    public AlterTableAddConstraint(SessionLocal session, Schema schema, int type, boolean ifNotExists) {
        super(session, schema);
        this.ifNotExists = ifNotExists;
        this.type = type;
    }

    private String generateConstraintName(Table table) {
        if (constraintName == null) {
            constraintName = getSchema().getUniqueConstraintName(session, table);
        }
        return constraintName;
    }

    @Override
    public long update(Table table) {
        try {
            return tryUpdate(table);
        } catch (DbException e) {
            try {
                if (createdUniqueConstraint != null) {
                    Index index = createdUniqueConstraint.getIndex();
                    getDatabase().removeSchemaObject(session, createdUniqueConstraint);
                    createdIndexes.remove(index);
                }
                for (Index index : createdIndexes) {
                    getDatabase().removeSchemaObject(session, index);
                }
            } catch (Throwable ex) {
                e.addSuppressed(ex);
            }
            throw e;
        } finally {
            getSchema().freeUniqueName(constraintName);
        }
    }

    /**
     * Try to execute the statement.
     *
     * @return the update count
     */
    private int tryUpdate(Table table) {
        if (constraintName != null && getSchema().findConstraint(session, constraintName) != null) {
            if (ifNotExists) {
                return 0;
            }
            /**
             * 1.4.200 and older databases don't always have a unique constraint
             * for each referential constraint, so these constraints are created
             * and they may use the same generated name as some other not yet
             * initialized constraint that may lead to a name conflict.
             */
            if (!session.isQuirksMode()) {
                throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraintName);
            }
            constraintName = null;
        }
        Database db = getDatabase();
        db.lockMeta(session);
        table.lock(session, Table.EXCLUSIVE_LOCK);
        Constraint constraint;
        switch (type) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
            IndexColumn.mapColumns(indexColumns, table);
            index = table.findPrimaryKey();
            ArrayList<Constraint> constraints = table.getConstraints();
            for (int i = 0; constraints != null && i < constraints.size(); i++) {
                Constraint c = constraints.get(i);
                if (Constraint.Type.PRIMARY_KEY == c.getConstraintType()) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            if (index != null) {
                // if there is an index, it must match with the one declared
                // we don't test ascending / descending
                IndexColumn[] pkCols = index.getIndexColumns();
                if (pkCols.length != indexColumns.length) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
                for (int i = 0; i < pkCols.length; i++) {
                    if (pkCols[i].column != indexColumns[i].column) {
                        throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                    }
                }
            } else {
                IndexType indexType = IndexType.createPrimaryKey(
                        table.isPersistIndexes(), primaryKeyHash);
                String indexName = table.getSchema().getUniqueIndexName(
                        session, table, Constants.PREFIX_PRIMARY_KEY);
                int indexId = getDatabase().allocateObjectId();
                try {
                    index = table.addIndex(session, indexName, indexId, indexColumns, indexColumns.length, indexType,
                            true, null);
                } finally {
                    getSchema().freeUniqueName(indexName);
                }
            }
            index.getIndexType().setBelongsToConstraint(true);
            int id = getObjectId();
            String name = generateConstraintName(table);
            ConstraintUnique pk = new ConstraintUnique(getSchema(), id, name, table, true, null);
            pk.setColumns(indexColumns);
            pk.setIndex(index, true);
            constraint = pk;
            break;
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE:
            if (indexColumns == null) {
                Column[] columns = table.getColumns();
                int columnCount = columns.length;
                ArrayList<IndexColumn> list = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    Column c = columns[i];
                    if (c.getVisible()) {
                        IndexColumn indexColumn = new IndexColumn(c.getName());
                        indexColumn.column = c;
                        list.add(indexColumn);
                    }
                }
                if (list.isEmpty()) {
                    throw DbException.get(ErrorCode.SYNTAX_ERROR_1, "UNIQUE(VALUE) on table without columns");
                }
                indexColumns = list.toArray(new IndexColumn[0]);
            } else {
                IndexColumn.mapColumns(indexColumns, table);
            }
            constraint = createUniqueConstraint(table, index, indexColumns, false);
            break;
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
            int id = getObjectId();
            String name = generateConstraintName(table);
            ConstraintCheck check = new ConstraintCheck(getSchema(), id, name, table);
            TableFilter filter = new TableFilter(session, table, null, false, null, 0, null);
            checkExpression.mapColumns(filter, 0, Expression.MAP_INITIAL);
            checkExpression = checkExpression.optimize(session);
            check.setExpression(checkExpression);
            check.setTableFilter(filter);
            constraint = check;
            if (checkExisting) {
                check.checkExistingData(session);
            }
            break;
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
            Table refTable = refSchema.resolveTableOrView(session, refTableName);
            if (refTable == null) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, refTableName);
            }
            if (refTable != table) {
                session.getUser().checkTableRight(refTable, Right.SCHEMA_OWNER);
            }
            if (!refTable.canReference()) {
                StringBuilder builder = new StringBuilder("Reference ");
                refTable.getSQL(builder, HasSQL.TRACE_SQL_FLAGS);
                throw DbException.getUnsupportedException(builder.toString());
            }
            boolean isOwner = false;
            IndexColumn.mapColumns(indexColumns, table);
            if (refIndexColumns == null) {
                refIndexColumns = refTable.getPrimaryKey().getIndexColumns();
            } else {
                IndexColumn.mapColumns(refIndexColumns, refTable);
            }
            int columnCount = indexColumns.length;
            if (refIndexColumns.length != columnCount) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
            for (IndexColumn indexColumn : indexColumns) {
                Column column = indexColumn.column;
                if (column.isGeneratedAlways()) {
                    switch (deleteAction) {
                    case SET_DEFAULT:
                    case SET_NULL:
                        throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_UPDATABLE_BY_CONSTRAINT_2,
                                column.getSQLWithTable(new StringBuilder(), HasSQL.TRACE_SQL_FLAGS).toString(),
                                "ON DELETE " + deleteAction.getSqlName());
                    default:
                        // All other actions are allowed
                    }
                    switch (updateAction) {
                    case CASCADE:
                    case SET_DEFAULT:
                    case SET_NULL:
                        throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_UPDATABLE_BY_CONSTRAINT_2,
                                column.getSQLWithTable(new StringBuilder(), HasSQL.TRACE_SQL_FLAGS).toString(),
                                "ON UPDATE " + updateAction.getSqlName());
                    default:
                        // All other actions are allowed
                    }
                }
            }
            for (int i = 0; i < columnCount; i++) {
                Column column1 = indexColumns[i].column, column2 = refIndexColumns[i].column;
                if (!DataType.areStableComparable(column1.getType(), column2.getType())) {
                    throw DbException.get(ErrorCode.UNCOMPARABLE_REFERENCED_COLUMN_2, column1.getCreateSQL(),
                            column2.getCreateSQL());
                }
            }
            ConstraintUnique unique = getUniqueConstraint(refTable, refIndexColumns);
            if (unique == null && !session.isQuirksMode()
                    && !session.getMode().createUniqueConstraintForReferencedColumns) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, IndexColumn.writeColumns(
                        new StringBuilder("PRIMARY KEY | UNIQUE ("), refIndexColumns, HasSQL.TRACE_SQL_FLAGS)
                        .append(')').toString());
            }
            if (index != null && canUseIndex(index, table, indexColumns, null)) {
                isOwner = true;
                index.getIndexType().setBelongsToConstraint(true);
            } else {
                index = getIndex(table, indexColumns, null);
                if (index == null) {
                    index = createIndex(table, indexColumns, null);
                    isOwner = true;
                }
            }
            int id = getObjectId();
            String name = generateConstraintName(table);
            ConstraintReferential refConstraint = new ConstraintReferential(getSchema(),
                    id, name, table);
            refConstraint.setColumns(indexColumns);
            refConstraint.setIndex(index, isOwner);
            refConstraint.setRefTable(refTable);
            refConstraint.setRefColumns(refIndexColumns);
            if (unique == null) {
                unique = createUniqueConstraint(refTable, refIndex, refIndexColumns, true);
                addConstraintToTable(db, refTable, unique);
                createdUniqueConstraint = unique;
            }
            refConstraint.setRefConstraint(unique);
            if (checkExisting) {
                refConstraint.checkExistingData(session);
            }
            refTable.addConstraint(refConstraint);
            refConstraint.setDeleteAction(deleteAction);
            refConstraint.setUpdateAction(updateAction);
            constraint = refConstraint;
            break;
        }
        default:
            throw DbException.getInternalError("type=" + type);
        }
        // parent relationship is already set with addConstraint
        constraint.setComment(comment);
        addConstraintToTable(db, table, constraint);
        return 0;
    }

    private ConstraintUnique createUniqueConstraint(Table table, Index index, IndexColumn[] indexColumns,
            boolean forForeignKey) {
        boolean isOwner = false;
        NullsDistinct needNullsDistinct = nullsDistinct != null ? nullsDistinct : NullsDistinct.DISTINCT;
        if (index != null && canUseIndex(index, table, indexColumns, needNullsDistinct)) {
            isOwner = true;
            index.getIndexType().setBelongsToConstraint(true);
        } else {
            index = getIndex(table, indexColumns, needNullsDistinct);
            if (index == null) {
                index = createIndex(table, indexColumns,
                        nullsDistinct != null ? nullsDistinct : session.getMode().nullsDistinct);
                isOwner = true;
            }
        }
        int id;
        String name;
        Schema tableSchema = table.getSchema();
        if (forForeignKey) {
            id = getDatabase().allocateObjectId();
            try {
                tableSchema.reserveUniqueName(constraintName);
                name = tableSchema.getUniqueConstraintName(session, table);
            } finally {
                tableSchema.freeUniqueName(constraintName);
            }
        } else {
            id = getObjectId();
            name = generateConstraintName(table);
        }
        if (indexColumns.length == 1 && needNullsDistinct == NullsDistinct.ALL_DISTINCT) {
            needNullsDistinct = NullsDistinct.DISTINCT;
        }
        ConstraintUnique unique = new ConstraintUnique(tableSchema, id, name, table, false, needNullsDistinct);
        unique.setColumns(indexColumns);
        unique.setIndex(index, isOwner);
        return unique;
    }

    private void addConstraintToTable(Database db, Table table, Constraint constraint) {
        if (table.isTemporary() && !table.isGlobalTemporary()) {
            session.addLocalTempTableConstraint(constraint);
        } else {
            db.addSchemaObject(session, constraint);
        }
        table.addConstraint(constraint);
    }

    private Index createIndex(Table t, IndexColumn[] cols, NullsDistinct nullsDistinct) {
        int indexId = getDatabase().allocateObjectId();
        IndexType indexType;
        if (nullsDistinct != null) {
            // for unique constraints
            indexType = IndexType.createUnique(t.isPersistIndexes(), false, cols.length, nullsDistinct);
        } else {
            // constraints
            indexType = IndexType.createNonUnique(t.isPersistIndexes());
        }
        indexType.setBelongsToConstraint(true);
        String prefix = constraintName == null ? "CONSTRAINT" : constraintName;
        String indexName = t.getSchema().getUniqueIndexName(session, t,
                prefix + "_INDEX_");
        try {
            Index index = t.addIndex(session, indexName, indexId, cols, nullsDistinct != null ? cols.length : 0,
                    indexType, true, null);
            createdIndexes.add(index);
            return index;
        } finally {
            getSchema().freeUniqueName(indexName);
        }
    }

    public void setDeleteAction(ConstraintActionType action) {
        this.deleteAction = action;
    }

    public void setUpdateAction(ConstraintActionType action) {
        this.updateAction = action;
    }

    private static ConstraintUnique getUniqueConstraint(Table t, IndexColumn[] cols) {
        ArrayList<Constraint> constraints = t.getConstraints();
        if (constraints != null) {
            for (Constraint constraint : constraints) {
                if (constraint.getTable() == t) {
                    Constraint.Type constraintType = constraint.getConstraintType();
                    if (constraintType == Constraint.Type.PRIMARY_KEY || constraintType == Constraint.Type.UNIQUE) {
                        if (canUseIndex(constraint.getIndex(), t, cols, NullsDistinct.DISTINCT)) {
                            return (ConstraintUnique) constraint;
                        }
                    }
                }
            }
        }
        return null;
    }

    private static Index getIndex(Table t, IndexColumn[] cols, NullsDistinct nullsDistinct) {
        ArrayList<Index> indexes = t.getIndexes();
        Index index = null;
        if (indexes != null) {
            for (Index idx : indexes) {
                if (canUseIndex(idx, t, cols, nullsDistinct)) {
                    if (index == null || idx.getIndexColumns().length < index.getIndexColumns().length) {
                        index = idx;
                    }
                }
            }
        }
        return index;
    }

    private static boolean canUseIndex(Index index, Table table, IndexColumn[] cols, NullsDistinct nullsDistinct) {
        if (index.getTable() != table) {
            return false;
        }
        int allowedColumns;
        if (nullsDistinct != null) {
            allowedColumns = index.getUniqueColumnCount();
            if (allowedColumns != cols.length) {
                return false;
            }
            if (index.getIndexType().getEffectiveNullsDistinct().compareTo(nullsDistinct) < 0) {
                return false;
            }
        } else {
            if (index.getCreateSQL() == null || (allowedColumns = index.getColumns().length) != cols.length) {
                return false;
            }
        }
        for (IndexColumn col : cols) {
            // all columns of the list must be part of the index
            int i = index.getColumnIndex(col.column);
            if (i < 0 || i >= allowedColumns) {
                return false;
            }
        }
        return true;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public String getConstraintName() {
        return constraintName;
    }

    @Override
    public int getType() {
        return type;
    }

    public void setNullsDistinct(NullsDistinct nullsDistinct) {
        this.nullsDistinct = nullsDistinct;
    }

    public void setCheckExpression(Expression expression) {
        this.checkExpression = expression;
    }

    public void setIndexColumns(IndexColumn[] indexColumns) {
        this.indexColumns = indexColumns;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    /**
     * Set the referenced table.
     *
     * @param refSchema the schema
     * @param ref the table name
     */
    public void setRefTableName(Schema refSchema, String ref) {
        this.refSchema = refSchema;
        this.refTableName = ref;
    }

    public void setRefIndexColumns(IndexColumn[] indexColumns) {
        this.refIndexColumns = indexColumns;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public void setRefIndex(Index refIndex) {
        this.refIndex = refIndex;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

    public void setPrimaryKeyHash(boolean b) {
        this.primaryKeyHash = b;
    }

}
