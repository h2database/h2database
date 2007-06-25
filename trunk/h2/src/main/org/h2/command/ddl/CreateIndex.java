/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * @author Thomas
 */
public class CreateIndex extends SchemaCommand {

    private String tableName;
    private String indexName;
    private String[] columnNames;
    private boolean primaryKey, unique, hash;
    private boolean ifNotExists;
    private String comment;

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

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public boolean getPrimaryKey() {
        return primaryKey;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public int update() throws SQLException {
        // TODO cancel: may support for index creation
        session.commit(true);
        Database db = session.getDatabase();
        boolean persistent = db.isPersistent();
        Table table = getSchema().getTableOrView(session, tableName);
        session.getUser().checkRight(table, Right.ALL);
        table.lock(session, true);
        if(!table.isPersistent()) {
            persistent = false;
        }
        int id = getObjectId(true, false);
        if(indexName == null) {
            indexName = getSchema().getUniqueIndexName("INDEX_");
        }
        if(getSchema().findIndex(indexName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(Message.INDEX_ALREADY_EXISTS_1, indexName);
        }
        IndexType indexType;
        if(primaryKey) {
            if(table.findPrimaryKey() != null) {
                throw Message.getSQLException(Message.SECOND_PRIMARY_KEY);
            }
            indexType = IndexType.createPrimaryKey(persistent, hash);
        } else if(unique) {
            indexType = IndexType.createUnique(persistent, hash);
        } else {
            indexType = IndexType.createNonUnique(persistent);
        }
        Column[] columns = table.getColumns(columnNames);
        table.addIndex(session, indexName, id, columns, indexType, headPos, comment);
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

}
