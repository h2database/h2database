/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

/**
 * Thread local hints for H2 query optimizer. All the ongoing queries in the
 * current thread will run with respect to these hints, so if they are needed
 * only for a single operation it is preferable to setup and drop them in
 * try-finally block.
 *
 * Currently works only in embedded mode.
 *
 * @author Sergi Vladykin
 */
public class OptimizerHints {

    private static final ThreadLocal<OptimizerHints> HINTS =
            new ThreadLocal<OptimizerHints>();

    private boolean joinReorderEnabled = true;

    /**
     * Set thread local hints or {@code null} to drop any existing hints.
     *
     * @param hints the hints
     */
    public static void set(OptimizerHints hints) {
        if (hints != null) {
            HINTS.set(hints);
        } else {
            HINTS.remove();
        }
    }

    /**
     * Get the current thread local hints or {@code null} if none.
     *
     * @return the hints
     */
    public static OptimizerHints get() {
        return HINTS.get();
    }

    /**
     * Set whether reordering of tables (or anything else in the {@code FROM}
     * clause) is enabled. By default is {@code true}.
     *
     * @param joinReorderEnabled Flag value.
     */
    public void setJoinReorderEnabled(boolean joinReorderEnabled) {
        this.joinReorderEnabled = joinReorderEnabled;
    }

    public boolean getJoinReorderEnabled() {
        return joinReorderEnabled;
    }
}
