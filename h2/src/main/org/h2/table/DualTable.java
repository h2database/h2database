/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.engine.Session;
import org.h2.index.DualIndex;
import org.h2.index.Index;
import org.h2.schema.Schema;
import org.h2.value.Value;

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
     * @param schema
     *            the schema (always the main schema)
     * @param noColumns
     *            whether this table has no columns
     */
    public DualTable(Schema schema, boolean noColumns) {
        super(schema, 0, NAME);
        setColumns(noColumns ? new Column[0] : new Column[] { new Column("X", Value.LONG) });
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
        return new DualIndex(this, IndexColumn.wrap(columns));
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
