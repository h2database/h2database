/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.query;

import org.h2.message.DbException;
import org.h2.util.HasSQL;
import org.h2.util.StringUtils;

/**
 * FOR UPDATE clause.
 */
public final class ForUpdate implements HasSQL {

    /**
     * Type of FOR UPDATE clause.
     */
    public enum Type {

        /**
         * Use default lock timeout.
         */
        DEFAULT,

        /**
         * Use specified lock timeout.
         */
        WAIT,

        /**
         * Use zero timeout.
         */
        NOWAIT,

        /**
         * Skip locked rows.
         */
        SKIP_LOCKED;

    }

    /**
     * FOR UPDATE clause without additional parameters.
     */
    public static final ForUpdate DEFAULT = new ForUpdate(Type.DEFAULT, -1);

    /**
     * FOR UPDATE NOWAIT clause.
     */
    public static final ForUpdate NOWAIT = new ForUpdate(Type.NOWAIT, 0);

    /**
     * FOR UPDATE SKIP LOCKED clause.
     */
    public static final ForUpdate SKIP_LOCKED = new ForUpdate(Type.SKIP_LOCKED, -2);

    /**
     * Returns FOR UPDATE WAIT N clause.
     *
     * @param timeoutMillis
     *            timeout in milliseconds
     * @return FOR UPDATE WAIT N clause
     */
    public static final ForUpdate wait(int timeoutMillis) {
        if (timeoutMillis < 0) {
            throw DbException.getInvalidValueException("timeout", timeoutMillis);
        }
        if (timeoutMillis == 0) {
            return NOWAIT;
        }
        return new ForUpdate(Type.WAIT, timeoutMillis);
    }

    private final Type type;

    private final int timeoutMillis;

    private ForUpdate(Type type, int timeoutMillis) {
        this.type = type;
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Returns type of FOR UPDATE clause.
     *
     * @return type of FOR UPDATE clause
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns timeout in milliseconds.
     *
     * @return timeout in milliseconds for {@link Type#WAIT}, {@code 0} for
     *         {@link Type#NOWAIT}, {@code -2} for {@link Type#SKIP_LOCKED},
     *         {@code -1} for default timeout
     */
    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append(" FOR UPDATE");
        switch (type) {
        case WAIT: {
            builder.append(" WAIT ").append(timeoutMillis / 1_000);
            int millis = timeoutMillis % 1_000;
            if (millis > 0) {
                StringUtils.appendZeroPadded(builder.append('.'), 3, millis);
            }
            break;
        }
        case NOWAIT:
            builder.append(" NOWAIT");
            break;
        case SKIP_LOCKED:
            builder.append(" SKIP LOCKED");
            break;
        default:
        }
        return builder;
    }

}
