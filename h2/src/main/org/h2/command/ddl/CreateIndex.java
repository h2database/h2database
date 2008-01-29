/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.IndexColumn;
import org.h2.table.Table;

/**
 * This class represents the statement
 * CREATE INDEX
 */
public class CreateIndex extends SchemaCommand {

    private String tableName;
    private String indexName;
    private IndexColumn[] indexColumns;
    private boolean primaryKey, unique, hash;
    private boolean ifNotExists;
    private String comment;
    private String constraintName;

    public CreateIndex(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexColumns(IndexColumn[] columns) {
        this.indexColumns = columns;
    }

    public boolean getPrimaryKey() {
        return primaryKey;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    private String generateConstraintName(DbObject obj, int id) throws SQLException {
        if (constraintName == null) {
            constraintName = getSchema().getUniqueConstraintName(obj);
        }
        return constraintName;
    }

    public int update() throws SQLException {
        // TODO cancel: may support for index creation
        session.commit(true);
        Database db = session.getDatabase();
        boolean persistent = db.isPersistent();
        Table table = getSchema().getTableOrView(session, tableName);
        session.getUser().checkRight(table, Right.ALL);
        table.lock(session, true, true);
        if (!table.isPersistent()) {
            persistent = false;
        }
        int id = getObjectId(true, false);
        if (indexName == null) {
            indexName = getSchema().getUniqueIndexName(table, "INDEX_");
        }
        if (getSchema().findIndex(indexName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(ErrorCode.INDEX_ALREADY_EXISTS_1, indexName);
        }
        IndexType indexType;
        if (primaryKey) {
            if (table.findPrimaryKey() != null) {
                throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
            }
            indexType = IndexType.createPrimaryKey(persistent, hash);
        } else if (unique) {
            indexType = IndexType.createUnique(persistent, hash);
        } else {
            indexType = IndexType.createNonUnique(persistent);
        }
        IndexColumn.mapColumns(indexColumns, table);
        Index index = table.addIndex(session, indexName, id, indexColumns, indexType, headPos, comment);
        int todo;
//        if (primaryKey) {
//            // TODO this code is a copy of CreateTable (primaryKey creation)
//            // for primary keys, create a constraint as well
//            String name = generateConstraintName(table, id);
//            int constraintId = getObjectId(true, true);
//            ConstraintUnique pk = new ConstraintUnique(getSchema(), constraintId, name, table, true);
//            pk.setColumns(index.getIndexColumns());
//            pk.setIndex(index, true);
//            pk.setComment(comment);
//            db.addSchemaObject(session, pk);
//            table.addConstraint(pk);
//        }
        return 0;
    }

    public void setPrimaryKey(boolean b) {
        this.primaryKey = b;
    }

    public void setUnique(boolean b) {
        this.unique = b;
    }

    public void setHash(boolean b) {
        this.hash = b;
    }

    public boolean getHash() {
        return hash;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

}
