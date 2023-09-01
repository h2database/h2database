/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

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

    /**
     * Static settings.
     */
    public static final class StaticSettings {

        /**
         * Whether unquoted identifiers are converted to upper case.
         */
        public final boolean databaseToUpper;

        /**
         * Whether unquoted identifiers are converted to lower case.
         */
        public final boolean databaseToLower;

        /**
         * Whether all identifiers are case insensitive.
         */
        public final boolean caseInsensitiveIdentifiers;

        /**
         * Creates new instance of static settings.
         *
         * @param databaseToUpper
         *            whether unquoted identifiers are converted to upper case
         * @param databaseToLower
         *            whether unquoted identifiers are converted to lower case
         * @param caseInsensitiveIdentifiers
         *            whether all identifiers are case insensitive
         */
        public StaticSettings(boolean databaseToUpper, boolean databaseToLower, boolean caseInsensitiveIdentifiers) {
            this.databaseToUpper = databaseToUpper;
            this.databaseToLower = databaseToLower;
            this.caseInsensitiveIdentifiers = caseInsensitiveIdentifiers;
        }

    }

    /**
     * Dynamic settings.
     */
    public static final class DynamicSettings {

        /**
         * The database mode.
         */
        public final Mode mode;

        /**
         * The current time zone.
         */
        public final TimeZoneProvider timeZone;

        /**
         * Creates new instance of dynamic settings.
         *
         * @param mode
         *            the database mode
         * @param timeZone
         *            the current time zone
         */
        public DynamicSettings(Mode mode, TimeZoneProvider timeZone) {
            this.mode = mode;
            this.timeZone = timeZone;
        }

    }

    private final ReentrantLock lock = new ReentrantLock();

    private ArrayList<String> sessionState;

    boolean sessionStateChanged;

    private boolean sessionStateUpdating;

    volatile StaticSettings staticSettings;

    Session() {
    }

    /**
     * Locks this session with a reentrant lock.
     *
     * <pre>
     * final Session session = ...;
     * session.lock();
     * try {
     *     ...
     * } finally {
     *     session.unlock();
     * }
     * </pre>
     */
    public final void lock() {
        lock.lock();
    }

    /**
     * Unlocks this session.
     *
     * @see #lock()
     */
    public final void unlock() {
        lock.unlock();
    }

    /**
     * Returns whether this session is locked by the current thread.
     *
     * @return {@code true} if it locked by the current thread, {@code false} if
     *         it is locked by another thread or is not locked at all
     */
    public final boolean isLockedByCurrentThread() {
        return lock.isHeldByCurrentThread();
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
    public abstract StaticSettings getStaticSettings();

    /**
     * Returns dynamic settings. These settings can be changed during lifecycle
     * of session.
     *
     * @return dynamic settings
     */
    public abstract DynamicSettings getDynamicSettings();

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
