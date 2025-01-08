/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.command.Prepared;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * Data change statement.
 */
public abstract class DataChangeStatement extends Prepared {

    private boolean isPrepared;

    /**
     * Creates new instance of DataChangeStatement.
     *
     * @param session
     *            the session
     */
    protected DataChangeStatement(SessionLocal session) {
        super(session);
    }

    @Override
    public final void prepare() {
        if (isPrepared) {
            return;
        }
        doPrepare();
        isPrepared = true;
    }

    abstract void doPrepare();

    /**
     * Return the name of this statement.
     *
     * @return the short name of this statement.
     */
    public abstract String getStatementName();

    /**
     * Return the target table.
     *
     * @return the target table
     */
    public abstract Table getTable();

    @Override
    public final boolean isTransactional() {
        return true;
    }

    @Override
    public final ResultInterface queryMeta() {
        return null;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public final long update() {
        return update(null, null);
    }

    /**
     * Execute the statement with specified delta change collector and collection mode.
     *
     * @param deltaChangeCollector
     *            target result
     * @param deltaChangeCollectionMode
     *            collection mode
     * @return the update count
     */
    public abstract long update(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode);

    protected final Row lockAndRecheckCondition(TableFilter targetTableFilter, Expression condition) {
        Table table = targetTableFilter.getTable();
        Row row = targetTableFilter.get();
        if (table.isRowLockable()) {
            Row lockedRow = table.lockRow(session, row, -1);
            if (lockedRow == null) {
                return null;
            }
            if (!row.hasSharedData(lockedRow)) {
                row = lockedRow;
                targetTableFilter.set(row);
                if (condition != null && !condition.getBooleanValue(session)) {
                    return null;
                }
            }
        }
        return row;
    }
}
