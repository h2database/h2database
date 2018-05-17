/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.lang.ref.WeakReference;

import org.h2.message.Trace;

/**
 * This class is responsible to close a database on JVM shutdown.
 */
class OnExitDatabaseCloser extends Thread {

    private final Trace trace;
    private volatile WeakReference<Database> databaseRef;

    OnExitDatabaseCloser(Database db) {
        databaseRef = new WeakReference<>(db);
        trace = db.getTrace(Trace.DATABASE);
        Runtime.getRuntime().addShutdownHook(this);
    }

    /**
     * Stop and disable the database closer. This method is called after the
     * database has been closed.
     */
    void reset() {
        databaseRef = null;
        try {
            Runtime.getRuntime().removeShutdownHook(this);
        } catch (IllegalStateException e) {
            // ignore
        } catch (SecurityException e) {
            // applets may not do that - ignore
        }
    }

    @Override
    public void run() {
        Database database;
        WeakReference<Database> ref = databaseRef;
        if (ref != null && (database = ref.get()) != null) {
            try {
                database.close(true);
            } catch (RuntimeException e) {
                // this can happen when stopping a web application,
                // if loading classes is no longer allowed
                // it would throw an IllegalStateException
                try {
                    trace.error(e, "could not close the database");
                    // if this was successful, we ignore the exception
                    // otherwise not
                } catch (Throwable e2) {
                    e.addSuppressed(e2);
                    throw e;
                }
            }
        }
    }

}
