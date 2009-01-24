/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.lang.ref.WeakReference;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.index.BtreeIndex;
import org.h2.log.LogSystem;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.table.Table;
import org.h2.util.FileUtils;
import org.h2.util.ObjectArray;

/**
 * The writer thread is responsible to flush the transaction log file from time
 * to time.
 */
public class WriterThread extends Thread {

    /**
     * The reference to the database.
     *
     * Thread objects are not garbage collected
     * until they returned from the run() method
     * (even if they where never started)
     * so if the connection was not closed,
     * the database object cannot get reclaimed
     * by the garbage collector if we use a hard reference.
     */
    private volatile WeakReference databaseRef;

    private int writeDelay;
    private long lastIndexFlush;
    private volatile boolean stop;
    private long oldLogFileDelete;
    private String oldLogFile;

    private WriterThread(Database database, int writeDelay) {
        this.databaseRef = new WeakReference(database);
        this.writeDelay = writeDelay;
    }

    /**
     * Change the write delay
     *
     * @param writeDelay the new write delay
     */
    public void setWriteDelay(int writeDelay) {
        LogSystem log = getLog();
        this.writeDelay = writeDelay;
        // TODO check if MIN_WRITE_DELAY is a good value
        if (writeDelay < SysProperties.MIN_WRITE_DELAY) {
            log.setFlushOnEachCommit(true);
        } else {
            log.setFlushOnEachCommit(false);
        }
    }

    /**
     * Create and start a new writer thread for the given database.
     *
     * @param database the database
     * @param writeDelay the delay
     * @return the writer thread object
     */
    public static WriterThread create(Database database, int writeDelay) {
        WriterThread thread = new WriterThread(database, writeDelay);
        thread.setName("H2 Log Writer " + database.getShortName());
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private LogSystem getLog() {
        Database database = (Database) databaseRef.get();
        if (database == null) {
            return null;
        }
        LogSystem log = database.getLog();
        return log;
    }

    private void flushIndexes(Database database) {
        long time = System.currentTimeMillis();
        if (lastIndexFlush + Constants.FLUSH_INDEX_DELAY > time) {
            return;
        }
        synchronized (database) {
            ObjectArray array = database.getAllSchemaObjects(DbObject.INDEX);
            for (int i = 0; i < array.size(); i++) {
                DbObject obj = (DbObject) array.get(i);
                if (obj instanceof BtreeIndex) {
                    BtreeIndex idx = (BtreeIndex) obj;
                    if (idx.getLastChange() == 0) {
                        continue;
                    }
                    Table tab = idx.getTable();
                    if (tab.isLockedExclusively()) {
                        continue;
                    }
                    if (idx.getLastChange() + Constants.FLUSH_INDEX_DELAY > time) {
                        continue;
                    }
                    try {
                        idx.flush(database.getSystemSession());
                    } catch (SQLException e) {
                        database.getTrace(Trace.DATABASE).error("flush index " + idx.getName(), e);
                    }
                }
            }
        }
        lastIndexFlush = time;
    }

    public void run() {
        while (!stop) {
            synchronized (this) {
                if (oldLogFile != null) {
                    long time = System.currentTimeMillis();
                    if (time > oldLogFileDelete) {
                        FileUtils.tryDelete(oldLogFile);
                        if (!FileUtils.exists(oldLogFile)) {
                            oldLogFile = null;
                        }
                    }
                }
            }
            Database database = (Database) databaseRef.get();
            if (database == null) {
                break;
            }
            if (Constants.FLUSH_INDEX_DELAY != 0) {
                flushIndexes(database);
            }
            LogSystem log = database.getLog();
            if (log == null) {
                break;
            }
            try {
                log.flush();
            } catch (SQLException e) {
                TraceSystem traceSystem = database.getTraceSystem();
                if (traceSystem != null) {
                    traceSystem.getTrace(Trace.LOG).error("flush", e);
                }
            }
            // TODO log writer: could also flush the dirty cache when there is
            // low activity
            int wait = writeDelay;
            if (wait < SysProperties.MIN_WRITE_DELAY) {
                // wait 0 mean wait forever, which is not what we want
                wait = SysProperties.MIN_WRITE_DELAY;
            }
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        databaseRef = null;
    }

    /**
     * Stop the thread. This method is called when closing the database. Old log
     * files are deleted as well.
     */
    public void stopThread() throws SQLException {
        stop = true;
        deleteLogFileLater(null);
    }

    /**
     * Delete the following log file later on. If there is already a file to be
     * deleted, that one will be deleted immediately.
     *
     * @param fileName the name of the file to delete
     */
    public synchronized void deleteLogFileLater(String fileName) throws SQLException {
        if (oldLogFile != null) {
            FileUtils.delete(oldLogFile);
        }
        int delay = SysProperties.getLogFileDeleteDelay();
        if (delay == 0 && fileName != null) {
            FileUtils.delete(fileName);
        } else {
            oldLogFile = fileName;
            if (fileName != null) {
                oldLogFileDelete = System.currentTimeMillis() + delay;
            }
        }
    }

}
