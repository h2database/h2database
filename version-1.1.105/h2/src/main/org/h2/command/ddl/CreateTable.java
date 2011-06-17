/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;

/**
 * This class represents the statement
 * CREATE TABLE
 */
public class CreateTable extends SchemaCommand {

    private String tableName;
    private ObjectArray constraintCommands = new ObjectArray();
    private ObjectArray columns = new ObjectArray();
    private IndexColumn[] pkColumns;
    private boolean ifNotExists;
    private boolean persistent = true;
    private boolean temporary;
    private boolean globalTemporary;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;
    private boolean clustered;

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

    /**
     * Add a column to this table.
     *
     * @param column the column to add
     */
    public void addColumn(Column column) {
        if (columns == null) {
            columns = new ObjectArray();
        }
        columns.add(column);
    }

    /**
     * Add a constraint statement to this statement.
     * The primary key definition is one possible constraint statement.
     *
     * @param command the statement to add
     */
    public void addConstraintCommand(Prepared command) throws SQLException {
        if (command instanceof CreateIndex) {
            constraintCommands.add(command);
        } else {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == AlterTableAddConstraint.PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public int update() throws SQLException {
        // TODO rights: what rights are required to create a table?
        session.commit(true);
        Database db = session.getDatabase();
        if (!db.isPersistent()) {
            persistent = false;
        }
        if (getSchema().findTableOrView(session, tableName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
        }
        if (asQuery != null) {
            asQuery.prepare();
            if (columns.size() == 0) {
                generateColumnsFromQuery();
            } else if (columns.size() != asQuery.getColumnCount()) {
                throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        if (pkColumns != null) {
            int len = pkColumns.length;
            for (int i = 0; i < columns.size(); i++) {
                Column c = (Column) columns.get(i);
                for (int j = 0; j < len; j++) {
                    if (c.getName().equals(pkColumns[j].columnName)) {
                        c.setNullable(false);
                    }
                }
            }
        }
        ObjectArray sequences = new ObjectArray();
        for (int i = 0; i < columns.size(); i++) {
            Column c = (Column) columns.get(i);
            if (c.getAutoIncrement()) {
                int objId = getObjectId(true, true);
                c.convertAutoIncrementToSequence(session, getSchema(), objId, temporary);
            }
            Sequence seq = c.getSequence();
            if (seq != null) {
                sequences.add(seq);
            }
        }
        int id = getObjectId(true, true);
        TableData table = getSchema().createTable(tableName, id, columns, persistent, clustered, headPos);
        table.setComment(comment);
        table.setTemporary(temporary);
        table.setGlobalTemporary(globalTemporary);
        if (temporary && !globalTemporary) {
            if (onCommitDrop) {
                table.setOnCommitDrop(true);
            }
            if (onCommitTruncate) {
                table.setOnCommitTruncate(true);
            }
            session.addLocalTempTable(table);
        } else {
            db.addSchemaObject(session, table);
        }
        try {
            for (int i = 0; i < columns.size(); i++) {
                Column c = (Column) columns.get(i);
                c.prepareExpression(session);
            }
            for (int i = 0; i < sequences.size(); i++) {
                Sequence sequence = (Sequence) sequences.get(i);
                table.addSequence(sequence);
            }
            for (int i = 0; i < constraintCommands.size(); i++) {
                Prepared command = (Prepared) constraintCommands.get(i);
                command.update();
            }
            if (asQuery != null) {
                boolean old = session.getUndoLogEnabled();
                try {
                    session.setUndoLogEnabled(false);
                    Insert insert = null;
                    insert = new Insert(session);
                    insert.setQuery(asQuery);
                    insert.setTable(table);
                    insert.prepare();
                    insert.update();
                } finally {
                    session.setUndoLogEnabled(old);
                }
            }
        } catch (SQLException e) {
            db.checkPowerOff();
            db.removeSchemaObject(session, table);
            throw e;
        }
        return 0;
    }

    private void generateColumnsFromQuery() {
        int columnCount = asQuery.getColumnCount();
        ObjectArray expressions = asQuery.getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = (Expression) expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0 || (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 || dt.defaultScale > scale)) {
                scale = dt.defaultScale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            addColumn(col);
        }
    }

    /**
     * Sets the primary key columns, but also check if a primary key
     * with different columns is already defined.
     *
     * @param columns the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(IndexColumn[] columns) throws SQLException {
        if (pkColumns != null) {
            if (columns.length != pkColumns.length) {
                throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
            }
            for (int i = 0; i < columns.length; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        this.globalTemporary = globalTemporary;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

}
