/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;

/**
 * @author Thomas
 */
public class CreateTable extends SchemaCommand {

    private String tableName;
    private ObjectArray constraintCommands = new ObjectArray();
    private ObjectArray columns = new ObjectArray();
    private String[] pkColumnNames;
    private boolean ifNotExists;
    private boolean persistent = true;
    private boolean hashPrimaryKey;
    private ObjectArray sequences;
    private boolean temporary;
    private boolean globalTemporary;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;

    public CreateTable(Session session, Schema schema) {
        super(session, schema);
    }

    public void setQuery(Query query) {
        this.asQuery = query;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public void addConstraintCommand(Prepared command) throws SQLException {
        if(command instanceof CreateIndex) {
            CreateIndex create = (CreateIndex) command;
            if(create.getPrimaryKey()) {
                setPrimaryKeyColumnNames(create.getColumnNames());
                setHashPrimaryKey(create.getHash());
            } else {
                constraintCommands.add(command);
            }
        } else {
            constraintCommands.add(command);
        }
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public int update() throws SQLException {
        // TODO rights: what rights are required to create a table?
        session.commit(true);
        Database db = session.getDatabase();
        if(!db.isPersistent()) {
            persistent = false;
        }
        if(getSchema().findTableOrView(session, tableName)!=null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(Message.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
        }
        if(asQuery != null) {
            generateColumnFromQuery();
        }
        if (pkColumnNames != null) {
            int len = pkColumnNames.length;
            for(int i=0; i<columns.size(); i++) {
                Column c = (Column) columns.get(i);
                for(int j=0; j<len; j++) {
                    if(c.getName().equals(pkColumnNames[j])) {
                        c.setNullable(false);
                    }
                }
            }
        }
        sequences = new ObjectArray();
        for(int i=0; i<columns.size(); i++) {
            Column c = (Column) columns.get(i);
            if(c.getAutoIncrement()) {
                int objId = getObjectId(true, true);
                c.convertAutoIncrementToSequence(session, getSchema(), objId, temporary);
            }
            Sequence seq = c.getSequence();
            if(seq != null) {
                sequences.add(seq);
            }
        }
        int id = getObjectId(true, true);
        TableData table = getSchema().createTable(tableName, id, columns, persistent);
        table.setComment(comment);
        table.setTemporary(temporary);
        table.setGlobalTemporary(globalTemporary);
        if(temporary && !globalTemporary) {
            if(onCommitDrop) {
                table.setOnCommitDrop(true);
            }
            if(onCommitTruncate) {
                table.setOnCommitTruncate(true);
            }
            session.addLocalTempTable(table);
        } else {
            db.addSchemaObject(session, table);
        }
        try {
            for(int i=0; i<columns.size(); i++) {
                Column c = (Column) columns.get(i);
                c.prepareExpression(session);
            }
            if (pkColumnNames != null) {
                Column[] pk =table.getColumns(pkColumnNames);
                int indexId = getObjectId(true, false);
                table.addIndex(session, null, indexId, pk, IndexType.createPrimaryKey(persistent, hashPrimaryKey), Index.EMPTY_HEAD, null);
            }
            for(int i=0; i<sequences.size(); i++) {
                Sequence sequence = (Sequence) sequences.get(i);
                table.addSequence(sequence);
            }
            for(int i=0; i<constraintCommands.size(); i++) {
                Prepared command = (Prepared) constraintCommands.get(i);
                command.update();
            }
            if(asQuery != null) {
                Insert insert = new Insert(session);
                insert.setTable(table);
                insert.setQuery(asQuery);
                insert.prepare();
                int todoSetCreateAsBatchSize;
                insert.update();
            }
        } catch(SQLException e) {
            db.checkPowerOff();
            db.removeSchemaObject(session, table);
            throw e;
        }
        return 0;
    }

    private void generateColumnFromQuery() throws SQLException {
        int columnCount = asQuery.getColumnCount();
        ObjectArray expressions = asQuery.getExpressions();
        for(int i=0; i<columnCount; i++) {
            Expression expr = (Expression) expressions.get(i);
            int type = expr.getType();
            String name = expr.getColumnName();
            long precision = expr.getPrecision();
            int scale = expr.getScale();
            Column col = new Column(name, type, precision, scale);
            addColumn(col);
        }
    }

    public void setPrimaryKeyColumnNames(String[] colNames) throws SQLException {
        if(pkColumnNames != null) {
            if(colNames.length != pkColumnNames.length) {
                throw Message.getSQLException(Message.SECOND_PRIMARY_KEY);
            }
            for(int i=0; i<colNames.length; i++) {
                if(!colNames[i].equals(pkColumnNames[i])) {
                    throw Message.getSQLException(Message.SECOND_PRIMARY_KEY);
                }
            }
        }
        this.pkColumnNames = colNames;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public void setHashPrimaryKey(boolean b) {
        this.hashPrimaryKey = b;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        this.globalTemporary = globalTemporary;
    }

    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
