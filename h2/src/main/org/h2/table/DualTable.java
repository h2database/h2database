/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.DualIndex;
import org.h2.index.Index;

/**
 * The DUAL table for selects without a FROM clause.
 */
public class DualTable extends VirtualTable {

    /**
     * The name of the range table.
     */
    public static final String NAME = "DUAL";

    /**
     * Create a new range with the given start and end expressions.
     *
     * @param database
     *            the database
     */
    public DualTable(Database database) {
        super(database.getMainSchema(), 0, NAME);
        setColumns(new Column[0]);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return builder.append(NAME);
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        return 1L;
    }

    @Override
    public TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    @Override
    public Index getScanIndex(Session session) {
        return new DualIndex(this);
    }

    @Override
    public long getMaxDataModificationId() {
        return 0L;
    }

    @Override
    public long getRowCountApproximation() {
        return 1L;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

}
