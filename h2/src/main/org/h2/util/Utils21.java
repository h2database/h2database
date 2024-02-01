/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * Utilities with specialized implementations for Java 21 and later versions.
 *
 * This class contains basic implementations for older versions of Java and it
 * is overridden in multi-release JARs.
 */
public final class Utils21 {

    /*
     * Signatures of methods should match with
     * h2/src/java21/src/org/h2/util/Utils21.java and precompiled
     * h2/src/java21/precompiled/org/h2/util/Utils21.class.
     */

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
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        return thread;
    }

    private Utils21() {
    }

}
