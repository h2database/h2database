/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.aggregate.AggregateDataSelectivity;
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * This class represents the statements
 * ANALYZE and ANALYZE TABLE
 */
public class Analyze extends DefineCommand {

    /**
     * The sample size.
     */
    private int sampleRows;
    /**
     * used in ANALYZE TABLE...
     */
    private Table table;

    public Analyze(Session session) {
        super(session);
        sampleRows = session.getDatabase().getSettings().analyzeSample;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (table != null) {
            analyzeTable(session, table, sampleRows, true);
        } else {
            for (Table table : db.getAllTablesAndViews(false)) {
                analyzeTable(session, table, sampleRows, true);
            }
        }
        return 0;
    }

    /**
     * Analyze this table.
     *
     * @param session the session
     * @param table the table
     * @param sample the number of sample rows
     * @param manual whether the command was called by the user
     */
    public static void analyzeTable(Session session, Table table, int sample,
                                    boolean manual) {
        if (table.getTableType() != TableType.TABLE ||
                table.isHidden() || session == null) {
            return;
        }
        if (!manual) {
            if (session.getDatabase().isSysTableLocked()) {
                return;
            }
            if (table.hasSelectTrigger()) {
                return;
            }
        }
        if (table.isTemporary() && !table.isGlobalTemporary()
                && session.findLocalTempTable(table.getName()) == null) {
            return;
        }
        if (table.isLockedExclusively() && !table.isLockedExclusivelyBy(session)) {
            return;
        }
        if (!session.getUser().hasRight(table, Right.SELECT)) {
            return;
        }
        if (session.getCancel() != 0) {
            // if the connection is closed and there is something to undo
            return;
        }
        table.lock(session, false, false);
        Column[] columns = table.getColumns();
        int columnCount = columns.length;
        if (columnCount == 0) {
            return;
        }
        AggregateDataSelectivity[] aggregates = new AggregateDataSelectivity[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Column col = columns[i];
            if (!DataType.isLargeObject(col.getType().getValueType())) {
                aggregates[i] = new AggregateDataSelectivity();
            }
        }
        Cursor cursor = table.getScanIndex(session).find(session, null, null);
        for (int rowNumber = 0; (sample <= 0 || rowNumber < sample) && cursor.next(); rowNumber++) {
            Row row = cursor.get();
            for (int i = 0; i < columnCount; i++) {
                AggregateDataSelectivity aggregate = aggregates[i];
                if (aggregate != null) {
                    aggregate.add(null, row.getValue(i));
                }
            }
        }
        for (int i = 0; i < columnCount; i++) {
            AggregateDataSelectivity aggregate = aggregates[i];
            if (aggregate != null) {
                columns[i].setSelectivity(aggregate.getValue(null, Value.INT).getInt());
            }
        }
        session.getDatabase().updateMeta(session, table);
    }

    public void setTop(int top) {
        this.sampleRows = top;
    }

    @Override
    public int getType() {
        return CommandInterface.ANALYZE;
    }

}
