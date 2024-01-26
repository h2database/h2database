/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statement
 * TRUNCATE TABLE
 */
public class TruncateTable extends DefineCommand {

    private Table table;

    private boolean restart;

    public TruncateTable(SessionLocal session) {
        super(session);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setRestart(boolean restart) {
        this.restart = restart;
    }

    @Override
    public long update() {
        if (!table.canTruncate()) {
            throw DbException.get(ErrorCode.CANNOT_TRUNCATE_1, table.getTraceSQL());
        }
        session.getUser().checkTableRight(table, Right.DELETE);
        table.lock(session, Table.EXCLUSIVE_LOCK);
        long result = table.truncate(session);
        if (restart) {
            for (Column column : table.getColumns()) {
                Sequence sequence = column.getSequence();
                if (sequence != null) {
                    sequence.modify(sequence.getStartValue(), null, null, null, null, null, null);
                    getDatabase().updateMeta(session, sequence);
                }
            }
        }
        return result;
    }

    @Override
    public int getType() {
        return CommandInterface.TRUNCATE_TABLE;
    }

}
