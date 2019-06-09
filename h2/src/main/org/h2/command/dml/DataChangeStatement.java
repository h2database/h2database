/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.result.ResultTarget;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;

/**
 * Data change statement.
 */
public interface DataChangeStatement {

    /**
     * Return the name of this statement.
     *
     * @return the short name of this statement.
     */
    String getStatementName();

    /**
     * Return the target table.
     *
     * @return the target table
     */
    Table getTable();

    /**
     * Get the SQL statement.
     *
     * @return the SQL statement
     */
    String getSQL();

    /**
     * Set the delta change collector and collection mode.
     *
     * @param deltaChangeCollector
     *            target result
     * @param deltaChangeCollectionMode
     *            collection mode
     */
    void setDeltaChangeCollector(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode);

    /**
     * Prepare this statement.
     */
    void prepare();

    /**
     * Execute the statement.
     *
     * @return the update count
     */
    int update();

}
