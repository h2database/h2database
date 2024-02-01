/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.h2.api.Trigger;
import org.h2.message.DbException;

/**
 * An adapter for the trigger interface that allows to use the ResultSet
 * interface instead of a row array.
 */
public abstract class TriggerAdapter implements Trigger {

    /**
     * The schema name.
     */
    protected String schemaName;

    /**
     * The name of the trigger.
     */
    protected String triggerName;

    /**
     * The name of the table.
     */
    protected String tableName;

    /**
     * Whether the fire method is called before or after the operation is
     * performed.
     */
    protected boolean before;

    /**
     * The trigger type: INSERT, UPDATE, DELETE, SELECT, or a combination (a bit
     * field).
     */
    protected int type;

    /**
     * This method is called by the database engine once when initializing the
     * trigger. It is called when the trigger is created, as well as when the
     * database is opened. The default implementation initialized the result
     * sets.
     *
     * @param conn a connection to the database
     * @param schemaName the name of the schema
     * @param triggerName the name of the trigger used in the CREATE TRIGGER
     *            statement
     * @param tableName the name of the table
     * @param before whether the fire method is called before or after the
     *            operation is performed
     * @param type the operation type: INSERT, UPDATE, DELETE, SELECT, or a
     *            combination (this parameter is a bit field)
     */
    @Override
    public void init(Connection conn, String schemaName,
            String triggerName, String tableName,
            boolean before, int type) throws SQLException {
        this.schemaName = schemaName;
        this.triggerName = triggerName;
        this.tableName = tableName;
        this.before = before;
        this.type = type;
    }

    @Override
    public final void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        throw DbException.getInternalError();
    }

    /**
     * This method is called for each triggered action by the default
     * fire(Connection conn, Object[] oldRow, Object[] newRow) method.
     * <p>
     * For "before" triggers, the new values of the new row may be changed
     * using the ResultSet.updateX methods.
     * </p>
     *
     * @param conn a connection to the database
     * @param oldRow the old row, or null if no old row is available (for
     *            INSERT)
     * @param newRow the new row, or null if no new row is available (for
     *            DELETE)
     * @throws SQLException if the operation must be undone
     */
    public abstract void fire(Connection conn, ResultSet oldRow,
            ResultSet newRow) throws SQLException;

}
