/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;

import org.h2.command.CommandInterface;
import org.h2.jdbc.meta.DatabaseMeta;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.store.DataHandler;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;
import org.h2.value.ValueLob;

/**
 * A local or remote session. A session represents a database connection.
 */
public abstract class Session implements CastDataProvider, AutoCloseable {

    private ArrayList<String> sessionState;

    boolean sessionStateChanged;

    private boolean sessionStateUpdating;

    volatile StaticSetting staticSettings;

    Session() {
    }

    /**
     * Get the list of the cluster servers for this session.
     *
     * @return A list of "ip:port" strings for the cluster servers in this
     *         session.
     */
    public abstract ArrayList<String> getClusterServers();

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    public abstract CommandInterface prepareCommand(String sql, int fetchSize);

    /**
     * Roll back pending transactions and close the session.
     */
    @Override
    public abstract void close();

    /**
     * Get the trace object
     *
     * @return the trace object
     */
    public abstract Trace getTrace();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    public abstract boolean isClosed();

    /**
     * Get the data handler object.
     *
     * @return the data handler
     */
    public abstract DataHandler getDataHandler();

    /**
     * Check whether this session has a pending transaction.
     *
     * @return true if it has
     */
    public abstract boolean hasPendingTransaction();

    /**
     * Cancel the current or next command (called when closing a connection).
     */
    public abstract void cancel();

    /**
     * Check if this session is in auto-commit mode.
     *
     * @return true if the session is in auto-commit mode
     */
    public abstract boolean getAutoCommit();

    /**
     * Set the auto-commit mode. This call doesn't commit the current
     * transaction.
     *
     * @param autoCommit the new value
     */
    public abstract void setAutoCommit(boolean autoCommit);

    /**
     * Add a temporary LOB, which is closed when the session commits.
     *
     * @param v the value
     * @return the specified value
     */
    public abstract ValueLob addTemporaryLob(ValueLob v);

    /**
     * Check if this session is remote or embedded.
     *
     * @return true if this session is remote
     */
    public abstract boolean isRemote();

    /**
     * Set current schema.
     *
     * @param schema the schema name
     */
    public abstract void setCurrentSchemaName(String schema);

    /**
     * Get current schema.
     *
     * @return the current schema name
     */
    public abstract String getCurrentSchemaName();

    /**
     * Sets the network connection information if possible.
     *
     * @param networkConnectionInfo the network connection information
     */
    public abstract void setNetworkConnectionInfo(NetworkConnectionInfo networkConnectionInfo);

    /**
     * Returns the isolation level.
     *
     * @return the isolation level
     */
    public abstract IsolationLevel getIsolationLevel();

    /**
     * Sets the isolation level.
     *
     * @param isolationLevel the isolation level to set
     */
    public abstract void setIsolationLevel(IsolationLevel isolationLevel);

    /**
     * Returns static settings. These settings cannot be changed during
     * lifecycle of session.
     *
     * @return static settings
     */
    public abstract StaticSetting getStaticSettings();

    /**
     * Returns dynamic settings. These settings can be changed during lifecycle
     * of session.
     *
     * @return dynamic settings
     */
    public abstract DynamicSetting getDynamicSettings();

    /**
     * Returns database meta information.
     *
     * @return database meta information
     */
    public abstract DatabaseMeta getDatabaseMeta();

    /**
     * Returns whether INFORMATION_SCHEMA contains old-style tables.
     *
     * @return whether INFORMATION_SCHEMA contains old-style tables
     */
    public abstract boolean isOldInformationSchema();

    /**
     * Re-create the session state using the stored sessionState list.
     */
    void recreateSessionState() {
        if (sessionState != null && !sessionState.isEmpty()) {
            sessionStateUpdating = true;
            try {
                for (String sql : sessionState) {
                    CommandInterface ci = prepareCommand(sql, Integer.MAX_VALUE);
                    ci.executeUpdate(null);
                }
            } finally {
                sessionStateUpdating = false;
                sessionStateChanged = false;
            }
        }
    }

    /**
     * Read the session state if necessary.
     */
    public void readSessionState() {
        if (!sessionStateChanged || sessionStateUpdating) {
            return;
        }
        sessionStateChanged = false;
        sessionState = Utils.newSmallArrayList();
        CommandInterface ci = prepareCommand(!isOldInformationSchema()
                ? "SELECT STATE_COMMAND FROM INFORMATION_SCHEMA.SESSION_STATE"
                : "SELECT SQL FROM INFORMATION_SCHEMA.SESSION_STATE", Integer.MAX_VALUE);
        ResultInterface result = ci.executeQuery(0, false);
        while (result.next()) {
            sessionState.add(result.currentRow()[0].getString());
        }
    }

    /**
     * Sets this session as thread local session, if this session is a local
     * session.
     *
     * @return old thread local session, or {@code null}
     */
    public Session setThreadLocalSession() {
        return null;
    }

    /**
     * Resets old thread local session.
     *
     * @param oldSession
     *            the old thread local session, or {@code null}
     */
    public void resetThreadLocalSession(Session oldSession) {
    }

}
