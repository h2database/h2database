/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.schema.Schema;

/**
 * A temporary shadow table for recursive queries.
 */
public class ShadowTable extends VirtualConstructedTable {

    public ShadowTable(Schema schema, String name, Column[] columns) {
        super(schema, 0, name);
        setColumns(columns);
    }

    @Override
    public ResultInterface getResult(SessionLocal session) {
        throw DbException.getInternalError("shadow table");
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return false;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return Long.MAX_VALUE;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return Long.MAX_VALUE;
    }

}
