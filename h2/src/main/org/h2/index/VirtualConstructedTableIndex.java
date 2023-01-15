/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.table.VirtualConstructedTable;

/**
 * An index for a virtual table that returns a result set. Search in this index
 * performs scan over all rows and should be avoided.
 */
public class VirtualConstructedTableIndex extends VirtualTableIndex {

    private final VirtualConstructedTable table;

    public VirtualConstructedTableIndex(VirtualConstructedTable table, IndexColumn[] columns) {
        super(table, null, columns);
        this.table = table;
    }

    @Override
    public boolean isFindUsingFullTableScan() {
        return true;
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
        return new VirtualTableCursor(this, first, last, table.getResult(session));
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        if (masks != null) {
            throw DbException.getUnsupportedException("Virtual table");
        }
        long expectedRows;
        if (table.canGetRowCount(session)) {
            expectedRows = table.getRowCountApproximation(session);
        } else {
            expectedRows = database.getSettings().estimatedFunctionTableRows;
        }
        return expectedRows * 10;
    }

    @Override
    public String getPlanSQL() {
        return table instanceof FunctionTable ? "function" : "table scan";
    }

    @Override
    public boolean canScan() {
        return false;
    }

}
