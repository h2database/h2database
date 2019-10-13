/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.Connection;

import org.h2.message.DbException;

/**
 * Level of isolation.
 */
public enum IsolationLevel {

    /**
     * Dirty reads, non-repeatable reads and phantom reads are allowed.
     */
    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED, Constants.LOCK_MODE_OFF),

    /**
     * Dirty reads aren't allowed; non-repeatable reads and phantom reads are
     * allowed.
     */
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED, Constants.LOCK_MODE_READ_COMMITTED),

    /**
     * Dirty reads and non-repeatable reads aren't allowed; phantom reads are
     * allowed.
     */
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ, Constants.LOCK_MODE_TABLE),

    /**
     * Dirty reads, non-repeatable reads and phantom reads are'n allowed.
     */
    SNAPSHOT(Constants.TRANSACTION_SNAPSHOT, Constants.LOCK_MODE_TABLE),

    /**
     * Dirty reads, non-repeatable reads and phantom reads are'n allowed.
     * Concurrent and serial execution of transactions with this isolation level
     * should have the same effect.
     */
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE, Constants.LOCK_MODE_TABLE);

    /**
     * Returns the isolation level from LOCK_MODE equivalent for PageStore and
     * old versions of H2.
     *
     * @param level
     *            the LOCK_MODE value
     * @return the isolation level
     */
    public static IsolationLevel fromJdbc(int level) {
        switch (level) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            return IsolationLevel.READ_UNCOMMITTED;
        case Connection.TRANSACTION_READ_COMMITTED:
            return IsolationLevel.READ_COMMITTED;
        case Connection.TRANSACTION_REPEATABLE_READ:
            return IsolationLevel.REPEATABLE_READ;
        case Constants.TRANSACTION_SNAPSHOT:
            return IsolationLevel.SNAPSHOT;
        case Connection.TRANSACTION_SERIALIZABLE:
            return IsolationLevel.SERIALIZABLE;
        default:
            throw DbException.getInvalidValueException("isolation level", level);
        }
    }

    /**
     * Returns the isolation level from LOCK_MODE equivalent for PageStore and
     * old versions of H2.
     *
     * @param lockMode
     *            the LOCK_MODE value
     * @return the isolation level
     */
    public static IsolationLevel fromLockMode(int lockMode) {
        switch (lockMode) {
        case Constants.LOCK_MODE_OFF:
            return IsolationLevel.READ_UNCOMMITTED;
        case Constants.LOCK_MODE_READ_COMMITTED:
        default:
            return IsolationLevel.READ_COMMITTED;
        case Constants.LOCK_MODE_TABLE:
        case Constants.LOCK_MODE_TABLE_GC:
            return IsolationLevel.SERIALIZABLE;
        }
    }

    /**
     * Returns the isolation level from its SQL name.
     *
     * @param sql
     *            the SQL name
     * @return the isolation level from its SQL name
     */
    public static IsolationLevel fromSql(String sql) {
        switch (sql) {
        case "READ UNCOMMITTED":
            return READ_UNCOMMITTED;
        case "READ COMMITTED":
            return READ_COMMITTED;
        case "REPEATABLE READ":
            return REPEATABLE_READ;
        case "SNAPSHOT":
            return SNAPSHOT;
        case "SERIALIZABLE":
            return SERIALIZABLE;
        default:
            throw DbException.getInvalidValueException("isolation level", sql);
        }
    }

    private final String sql;

    private final int jdbc, lockMode;

    private IsolationLevel(int jdbc, int lockMode) {
        sql = name().replace('_', ' ').intern();
        this.jdbc = jdbc;
        this.lockMode = lockMode;
    }

    /**
     * Returns the SQL representation of this isolation level.
     *
     * @return SQL representation of this isolation level
     */
    public String getSQL() {
        return sql;
    }

    /**
     * Returns the JDBC constant for this isolation level.
     *
     * @return the JDBC constant for this isolation level
     */
    public int getJdbc() {
        return jdbc;
    }

    /**
     * Returns the LOCK_MODE equivalent for PageStore and old versions of H2.
     *
     * @return the LOCK_MODE equivalent
     */
    public int getLockMode() {
        return lockMode;
    }

    /**
     * Returns whether a non-repeatable read phenomena is allowed.
     *
     * @return whether a non-repeatable read phenomena is allowed
     */
    public boolean allowNonRepeatableRead() {
        return ordinal() < REPEATABLE_READ.ordinal();
    }

}
