/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A class that implements this interface can be used as a trigger.
 */
public interface Trigger {

    /**
     * The trigger is called for INSERT statements.
     */
    int INSERT = 1;

    /**
     * The trigger is called for UPDATE statements.
     */
    int UPDATE = 2;

    /**
     * The trigger is called for DELETE statements.
     */
    int DELETE = 4;

    /**
     * This method is called by the database engine once when initializing the
     * trigger.
     *
     * @param conn a connection to the database
     * @param schemaName the name of the schema
     * @param triggerName the name of the trigger used in the CREATE TRIGGER
     *            statement
     * @param tableName the name of the table
     * @param before whether the fire method is called before or after the
     *            operation is performed
     * @param type the operation type: INSERT, UPDATE, or DELETE
     */
    void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
        throws SQLException;

    /**
     * This method is called for each triggered action.
     *
     * @param conn a connection to the database
     * @param oldRow the old row, or null if no old row is available (for
     *            INSERT)
     * @param newRow the new row, or null if no new row is available (for
     *            DELETE)
     * @throws SQLException if the operation must be undone
     */
    void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException;

}
