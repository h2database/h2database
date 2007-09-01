/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;

import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;

/**
 * @author Thomas
 */

public class AlterTableAddConstraint extends SchemaCommand {

    public static final int CHECK = 0, UNIQUE = 1, REFERENTIAL = 2, PRIMARY_KEY = 3;
    private int type;
    private String constraintName;
    private String tableName;
    private String[] columnNames;
    private int deleteAction;
    private int updateAction;
    private Schema refSchema;
    private String refTableName;
    private String[] refColumnNames;
    private Expression checkExpression;
    private Index index, refIndex;
    private String comment;
    private boolean checkExisting;

    public AlterTableAddConstraint(Session session, Schema schema) {
        super(session, schema);
    }

    private String generateConstraintName(int id) throws SQLException {
        if (constraintName == null) {
            constraintName = getSchema().getUniqueConstraintName();
        }
        return constraintName;
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        Table table = getSchema().getTableOrView(session, tableName);
        if (getSchema().findConstraint(constraintName) != null) {
            throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraintName);
        }
        Constraint constraint;
        session.getUser().checkRight(table, Right.ALL);
        table.lock(session, true, true);
        switch (type) {
        case CHECK: {
            int id = getObjectId(true, true);
            String name = generateConstraintName(id);
            ConstraintCheck check = new ConstraintCheck(getSchema(), id, name, table);
            TableFilter filter = new TableFilter(session, table, null, false, null);
            checkExpression.mapColumns(filter, 0);
            checkExpression = checkExpression.optimize(session);
            check.setExpression(checkExpression);
            check.setTableFilter(filter);
            constraint = check;
            if (checkExisting) {
                check.checkExistingData(session);
            }
            break;
        }
        case UNIQUE: {
            Column[] columns = table.getColumns(columnNames);
            boolean isOwner = false;
            if (index != null && canUseUniqueIndex(index, table, columns)) {
                isOwner = true;
                index.getIndexType().setBelongsToConstraint(true);
            } else {
                index = getUniqueIndex(table, columns);
                if (index == null) {
                    index = createIndex(table, columns, true);
                    isOwner = true;
                }
            }
            int id = getObjectId(true, true);
            String name = generateConstraintName(id);
            ConstraintUnique unique = new ConstraintUnique(getSchema(), id, name, table);
            unique.setColumns(columns);
            unique.setIndex(index, isOwner);
            constraint = unique;
            break;
        }
        case REFERENTIAL: {
            Table refTable = refSchema.getTableOrView(session, refTableName);
            session.getUser().checkRight(refTable, Right.ALL);
            boolean isOwner = false;
            Column[] columns = table.getColumns(columnNames);
            if (index != null && canUseIndex(index, table, columns)) {
                isOwner = true;
                index.getIndexType().setBelongsToConstraint(true);
            } else {
                index = getIndex(table, columns);
                if (index == null) {
                    index = createIndex(table, columns, false);
                    isOwner = true;
                }
            }
            Column[] refColumns;
            if (refColumnNames == null) {
                Index refIdx = refTable.getPrimaryKey();
                refColumns = refIdx.getColumns();
            } else {
                refColumns = refTable.getColumns(refColumnNames);
            }
            if (refColumns.length != columns.length) {
                throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
            boolean isRefOwner = false;
            if (refIndex != null && refIndex.getTable() == refTable) {
                isRefOwner = true;
                refIndex.getIndexType().setBelongsToConstraint(true);
            } else {
                refIndex = null;
            }
            if (refIndex == null) {
                refIndex = getUniqueIndex(refTable, refColumns);
                if (refIndex == null) {
                    refIndex = createIndex(refTable, refColumns, true);
                    isRefOwner = true;
                }
            }
            int id = getObjectId(true, true);
            String name = generateConstraintName(id);
            ConstraintReferential ref = new ConstraintReferential(getSchema(), id, name, table);
            ref.setColumns(columns);
            ref.setIndex(index, isOwner);
            ref.setRefTable(refTable);
            ref.setRefColumns(refColumns);
            ref.setRefIndex(refIndex, isRefOwner);
            if (checkExisting) {
                ref.checkExistingData(session);
            }
            constraint = ref;
            refTable.addConstraint(constraint);
            ref.setDeleteAction(session, deleteAction);
            ref.setUpdateAction(session, updateAction);
            break;
        }
        default:
            throw Message.getInternalError("type=" + type);
        }
        // parent relationship is already set with addConstraint
        constraint.setComment(comment);
        db.addSchemaObject(session, constraint);
        table.addConstraint(constraint);
        return 0;
    }

    private Index createIndex(Table t, Column[] cols, boolean unique) throws SQLException {
        int indexId = getObjectId(true, false);
        IndexType indexType;
        if (unique) {
            // TODO default index (hash or not; memory or not or same as table)
            // for unique constraints
            indexType = IndexType.createUnique(t.isPersistent(), false);
        } else {
            // TODO default index (memory or not or same as table) for unique
            // constraints
            indexType = IndexType.createNonUnique(t.isPersistent());
        }
        indexType.setBelongsToConstraint(true);
        String prefix = constraintName == null ? "CONSTRAINT" : constraintName;
        String indexName = getSchema().getUniqueIndexName(prefix + "_INDEX_");
        return t.addIndex(session, indexName, indexId, cols, indexType, Index.EMPTY_HEAD, null);
    }

    public void setDeleteAction(int action) {
        this.deleteAction = action;
    }

    public void setUpdateAction(int action) {
        this.updateAction = action;
    }

    private Index getUniqueIndex(Table t, Column[] cols) {
        ObjectArray list = t.getIndexes();
        for (int i = 0; i < list.size(); i++) {
            Index index = (Index) list.get(i);
            if (canUseUniqueIndex(index, t, cols)) {
                return index;
            }
        }
        return null;
    }

    private Index getIndex(Table t, Column[] cols) {
        ObjectArray list = t.getIndexes();
        for (int i = 0; i < list.size(); i++) {
            Index index = (Index) list.get(i);
            if (canUseIndex(index, t, cols)) {
                return index;
            }
        }
        return null;
    }

    private boolean canUseUniqueIndex(Index index, Table table, Column[] cols) {
        if (index.getTable() != table || !index.getIndexType().isUnique()) {
            return false;
        }
        if (index.getIndexType().belongsToConstraint()) {
            // the constraint might be dropped (also in an alter table
            // statement)
            return false;
        }
        Column[] indexCols = index.getColumns();
        if (indexCols.length > cols.length) {
            return false;
        }
        HashSet set = new HashSet(Arrays.asList(cols));
        for (int j = 0; j < indexCols.length; j++) {
            // all columns of the index must be part of the list,
            // but not all columns of the list need to be part of the index
            if (!set.contains(indexCols[j])) {
                return false;
            }
        }
        return true;
    }

    private boolean canUseIndex(Index index, Table table, Column[] cols) {
        if (index.getTable() != table || index.getCreateSQL() == null) {
            // can't use the scan index or index of another table
            return false;
        }
        if (index.getIndexType().belongsToConstraint()) {
            // the constraint might be dropped (also in an alter table
            // statement)
            return false;
        }
        Column[] indexCols = index.getColumns();
        if (indexCols.length < cols.length) {
            return false;
        }
        for (int j = 0; j < cols.length; j++) {
            // all columns of the list must be part of the index,
            // but not all columns of the index need to be part of the list
            // holes are not allowed (index=a,b,c & list=a,b is ok; but list=a,c
            // is not)
            int idx = index.getColumnIndex(cols[j]);
            if (idx < 0 || idx >= cols.length) {
                return false;
            }
        }
        return true;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setCheckExpression(Expression expression) {
        this.checkExpression = expression;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public void setRefTableName(Schema refSchema, String ref) {
        this.refSchema = refSchema;
        this.refTableName = ref;
    }

    public void setRefColumnNames(String[] cols) {
        this.refColumnNames = cols;
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

}
