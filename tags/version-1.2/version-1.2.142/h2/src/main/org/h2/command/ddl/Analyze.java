/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.Prepared;
import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.table.RegularTable;
import org.h2.table.Table;
import org.h2.util.StatementBuilder;

/**
 * This class represents the statement
 * ANALYZE
 */
public class Analyze extends DefineCommand {

    /**
     * The sample size.
     */
    private int sampleRows = SysProperties.ANALYZE_SAMPLE;

    public Analyze(Session session) {
        super(session);
    }

    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        // TODO do we need to lock the table?
        for (Table table : db.getAllTablesAndViews(false)) {
            analyzeTable(session, table, sampleRows, true);
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
    public static void analyzeTable(Session session, Table table, int sample, boolean manual) {
        if (!(table instanceof RegularTable) || table.isHidden() || session == null) {
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
        Database db = session.getDatabase();
        StatementBuilder buff = new StatementBuilder("SELECT ");
        Column[] columns = table.getColumns();
        for (Column col : columns) {
            buff.appendExceptFirst(", ");
            buff.append("SELECTIVITY(").append(col.getSQL()).append(')');
        }
        buff.append(" FROM ").append(table.getSQL());
        if (sample > 0) {
            buff.append(" LIMIT 1 SAMPLE_SIZE ").append(sample);
        }
        String sql = buff.toString();
        Prepared command = session.prepare(sql);
        ResultInterface result = command.query(0);
        result.next();
        for (int j = 0; j < columns.length; j++) {
            int selectivity = result.currentRow()[j].getInt();
            columns[j].setSelectivity(selectivity);
        }
        if (manual) {
            db.update(session, table);
        } else {
            Session s = db.getSystemSession();
            db.update(s, table);
            s.commit(true);
        }
    }

    public void setTop(int top) {
        this.sampleRows = top;
    }

}
