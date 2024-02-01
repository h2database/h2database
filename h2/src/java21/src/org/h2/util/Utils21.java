/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.Thread.Builder.OfVirtual;

/**
 * Utilities with specialized implementations for Java 21 and later versions.
 *
 * This class contains basic implementations for older versions of Java and it
 * is overridden in multi-release JARs.
 */
public final class Utils21 {

    private static final OfVirtual VIRTUAL_THREAD_BUILDER = Thread.ofVirtual();

    /**
     * Creates a new virtual thread (on Java 21+) for the specified task. Use
     * {@link Thread#start()} to schedule the thread to execute. On older
     * versions of Java a platform daemon thread is created instead.
     *
     * @param task
     *            the object to run
     * @return a new thread
     */
    public static Thread newVirtualThread(Runnable task) {
        return VIRTUAL_THREAD_BUILDER.unstarted(task);
    }

    private Utils21() {
    }

}
