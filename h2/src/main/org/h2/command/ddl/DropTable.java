/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.MaterializedView;
import org.h2.table.Table;
import org.h2.table.TableView;
import org.h2.util.Utils;

/**
 * This class represents the statement
 * DROP TABLE
 */
public class DropTable extends DefineCommand {

    private boolean ifExists;
    private ConstraintActionType dropAction;

    private final ArrayList<SchemaAndTable> tables = Utils.newSmallArrayList();

    public DropTable(SessionLocal session) {
        super(session);
        dropAction = getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT :
                    ConstraintActionType.CASCADE;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    /**
     * Add a table to drop.
     *
     * @param schema the schema
     * @param tableName the table name
     */
    public void addTable(Schema schema, String tableName) {
        tables.add(new SchemaAndTable(schema, tableName));
    }

    private boolean prepareDrop() {
        HashSet<Table> tablesToDrop = new HashSet<>();
        for (SchemaAndTable schemaAndTable : tables) {
            String tableName = schemaAndTable.tableName;
            Table table = schemaAndTable.schema.findTableOrView(session, tableName);
            if (table == null) {
                if (!ifExists) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
                }
            } else {
                session.getUser().checkTableRight(table, Right.SCHEMA_OWNER);
                if (!table.canDrop()) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
                }
                tablesToDrop.add(table);
            }
        }
        if (tablesToDrop.isEmpty()) {
            return false;
        }
        for (Table table : tablesToDrop) {
            ArrayList<String> dependencies = new ArrayList<>();
            if (dropAction == ConstraintActionType.RESTRICT) {
                CopyOnWriteArrayList<TableView> dependentViews = table.getDependentViews();
                if (dependentViews != null && !dependentViews.isEmpty()) {
                    for (TableView v : dependentViews) {
                        if (!tablesToDrop.contains(v)) {
                            dependencies.add(v.getName());
                        }
                    }
                }
                CopyOnWriteArrayList<MaterializedView> dependentMaterializedViews = table
                        .getDependentMaterializedViews();
                if (dependentMaterializedViews != null && !dependentMaterializedViews.isEmpty()) {
                    for (MaterializedView v : dependentMaterializedViews) {
                        if (!tablesToDrop.contains(v)) {
                            dependencies.add(v.getName());
                        }
                    }
                }
                final List<Constraint> constraints = table.getConstraints();
                if (constraints != null && !constraints.isEmpty()) {
                    for (Constraint c : constraints) {
                        if (!tablesToDrop.contains(c.getTable())) {
                            dependencies.add(c.getName());
                        }
                    }
                }
                if (!dependencies.isEmpty()) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_2, table.getName(),
                            String.join(", ", new HashSet<>(dependencies)));
                }
            }
            table.lock(session, Table.EXCLUSIVE_LOCK);
        }
        return true;
    }

    private void executeDrop() {
        for (SchemaAndTable schemaAndTable : tables) {
            // need to get the table again, because it may be dropped already
            // meanwhile (dependent object, or same object)
            Table table = schemaAndTable.schema.findTableOrView(session, schemaAndTable.tableName);
            if (table != null) {
                table.setModified();
                Database db = getDatabase();
                db.lockMeta(session);
                db.removeSchemaObject(session, table);
            }
        }
    }

    @Override
    public long update() {
        if (prepareDrop()) {
            executeDrop();
        }
        return 0;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TABLE;
    }

    private static final class SchemaAndTable {

        final Schema schema;

        final String tableName;

        SchemaAndTable(Schema schema, String tableName) {
            this.schema = schema;
            this.tableName = tableName;
        }

    }

}
