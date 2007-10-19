/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.lang.ref.WeakReference;

public class DatabaseCloser extends Thread {

    private final boolean shutdownHook;
    private volatile WeakReference databaseRef;
    private int delayInMillis;
    private boolean stopImmediately;

    DatabaseCloser(Database db, int delayInMillis, boolean shutdownHook) {
        this.databaseRef = new WeakReference(db);
        this.delayInMillis = delayInMillis;
        this.shutdownHook = shutdownHook;
    }

    public void reset() {
        synchronized (this) {
            databaseRef = null;
        }
        if (getThreadGroup().activeCount() > 100) {
            // in JDK 1.4 and below, all Thread objects are added to the ThreadGroup, 
            // and cause a memory leak if never started.
            // Need to start it, otherwise it leaks memory in JDK 1.4 and below
            stopImmediately = true;
            try {
                start();
            } catch (Throwable e) {
                // ignore
            }
        }
    }

    public void run() {
        if (stopImmediately) {
            return;
        }
        while (delayInMillis > 0) {
            try {
                int step = 100;
                Thread.sleep(step);
                delayInMillis -= step;
            } catch (Exception e) {
                // ignore
            }
            if (databaseRef == null) {
                return;
            }
        }
        synchronized (this) {
            if (databaseRef != null) {
                Database database = (Database) databaseRef.get();
                if (database != null) {
                    database.close(shutdownHook);
                }
            }
        }
    }

}
