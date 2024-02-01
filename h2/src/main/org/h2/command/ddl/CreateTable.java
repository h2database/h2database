/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.Insert;
import org.h2.command.query.Query;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * This class represents the statement
 * CREATE TABLE
 */
public class CreateTable extends CommandWithColumns {

    private final CreateTableData data = new CreateTableData();
    private boolean ifNotExists;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;
    private boolean withNoData;

    public CreateTable(SessionLocal session, Schema schema) {
        super(session, schema);
        data.persistIndexes = true;
        data.persistData = true;
    }

    public void setQuery(Query query) {
        this.asQuery = query;
    }

    public void setTemporary(boolean temporary) {
        data.temporary = temporary;
    }

    public void setTableName(String tableName) {
        data.tableName = tableName;
    }

    @Override
    public void addColumn(Column column) {
        data.columns.add(column);
    }

    public ArrayList<Column> getColumns() {
        return data.columns;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public long update() {
        Schema schema = getSchema();
        boolean isSessionTemporary = data.temporary && !data.globalTemporary;
        Database db = getDatabase();
        String tableEngine = data.tableEngine;
        if (tableEngine != null || db.getSettings().defaultTableEngine != null) {
            session.getUser().checkAdmin();
        } else if (!isSessionTemporary) {
            session.getUser().checkSchemaOwner(schema);
        }
        if (!db.isPersistent()) {
            data.persistIndexes = false;
        }
        if (!isSessionTemporary) {
            db.lockMeta(session);
        }
        if (schema.resolveTableOrView(session, data.tableName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.tableName);
        }
        if (asQuery != null) {
            asQuery.prepare();
            if (data.columns.isEmpty()) {
                generateColumnsFromQuery();
            } else if (data.columns.size() != asQuery.getColumnCount()) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            } else {
                ArrayList<Column> columns = data.columns;
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    if (column.getType().getValueType() == Value.UNKNOWN) {
                        columns.set(i, new Column(column.getName(), asQuery.getExpressions().get(i).getType()));
                    }
                }
            }
        }
        changePrimaryKeysToNotNull(data.columns);
        data.id = getObjectId();
        data.session = session;
        Table table = schema.createTable(data);
        ArrayList<Sequence> sequences = generateSequences(data.columns, data.temporary);
        table.setComment(comment);
        if (isSessionTemporary) {
            if (onCommitDrop) {
                table.setOnCommitDrop(true);
            }
            if (onCommitTruncate) {
                table.setOnCommitTruncate(true);
            }
            session.addLocalTempTable(table);
        } else {
            db.lockMeta(session);
            db.addSchemaObject(session, table);
        }
        try {
            for (Column c : data.columns) {
                c.prepareExpressions(session);
            }
            for (Sequence sequence : sequences) {
                table.addSequence(sequence);
            }
            createConstraints();
            HashSet<DbObject> set = new HashSet<>();
            table.addDependencies(set);
            for (DbObject obj : set) {
                if (obj == table) {
                    continue;
                }
                if (obj.getType() == DbObject.TABLE_OR_VIEW) {
                    if (obj instanceof Table) {
                        Table t = (Table) obj;
                        if (t.getId() > table.getId()) {
                            throw DbException.get(
                                    ErrorCode.FEATURE_NOT_SUPPORTED_1,
                                    "Table depends on another table " +
                                    "with a higher ID: " + t +
                                    ", this is currently not supported, " +
                                    "as it would prevent the database from " +
                                    "being re-opened");
                        }
                    }
                }
            }
            if (asQuery != null && !withNoData) {
                insertAsData(isSessionTemporary, db, table);
            }
        } catch (DbException e) {
            try {
                db.checkPowerOff();
                db.removeSchemaObject(session, table);
                if (!transactional) {
                    session.commit(true);
                }
            } catch (Throwable ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
        return 0;
    }

    /** This is called from REFRESH MATERIALIZED VIEW */
    void insertAsData(Table table) {
        insertAsData(false, getDatabase(), table);
    }

    /** Insert data for the CREATE TABLE .. AS */
    private void insertAsData(boolean isSessionTemporary, Database db, Table table) {
        boolean flushSequences = false;
        if (!isSessionTemporary) {
            db.unlockMeta(session);
            for (Column c : table.getColumns()) {
                Sequence s = c.getSequence();
                if (s != null) {
                    flushSequences = true;
                    s.setTemporary(true);
                }
            }
        }
        try {
            session.startStatementWithinTransaction(null);
            Insert insert = new Insert(session);
            insert.setQuery(asQuery);
            insert.setTable(table);
            insert.setInsertFromSelect(true);
            insert.prepare();
            insert.update();
        } finally {
            session.endStatement();
        }
        if (flushSequences) {
            db.lockMeta(session);
            for (Column c : table.getColumns()) {
                Sequence s = c.getSequence();
                if (s != null) {
                    s.setTemporary(false);
                    s.flush(session);
                }
            }
        }
    }

    private void generateColumnsFromQuery() {
        int columnCount = asQuery.getColumnCount();
        ArrayList<Expression> expressions = asQuery.getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            addColumn(new Column(expr.getColumnNameForView(session, i), expr.getType()));
        }
    }

    public void setPersistIndexes(boolean persistIndexes) {
        data.persistIndexes = persistIndexes;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        data.globalTemporary = globalTemporary;
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

    public void setPersistData(boolean persistData) {
        data.persistData = persistData;
        if (!persistData) {
            data.persistIndexes = false;
        }
    }

    public void setWithNoData(boolean withNoData) {
        this.withNoData = withNoData;
    }

    public void setTableEngine(String tableEngine) {
        data.tableEngine = tableEngine;
    }

    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        data.tableEngineParams = tableEngineParams;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_TABLE;
    }

}
