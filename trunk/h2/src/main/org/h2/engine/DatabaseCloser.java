/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.lang.ref.WeakReference;

public class DatabaseCloser extends Thread {
	
    private final boolean shutdownHook;
    private WeakReference databaseRef;
    private int delayInMillis;
    
    DatabaseCloser(Database db, int delayInMillis, boolean shutdownHook) {
        this.databaseRef = new WeakReference(db);
        this.delayInMillis = delayInMillis;
        this.shutdownHook = shutdownHook;
    }
    
    public void reset() {
        synchronized(this) {
            databaseRef = null;
        }
    }
    
    public void run() {
        while(delayInMillis > 0) {
            try {
                int step = 100;
                Thread.sleep(step);
                delayInMillis -= step;
            } catch(Exception e) {
                // ignore
            }
            if(databaseRef == null) {
                return;
            }
        }
        synchronized(this) {
            if(databaseRef != null) {
                Database database = (Database) databaseRef.get();
                if(database != null) {
                    database.close(shutdownHook);
                }
            }
        }
    }
    
}
