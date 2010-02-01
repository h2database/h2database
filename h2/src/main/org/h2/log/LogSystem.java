/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import java.sql.SQLException;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.store.PageStore;
import org.h2.util.ObjectArray;

/**
 * The transaction log system is responsible for the write ahead log mechanism
 * used in this database.
 */
public class LogSystem {

    /**
     * This special log position means that the log entry has been written.
     */
    public static final int LOG_WRITTEN = -1;

    private Database database;
    // TODO log file / deleteOldLogFilesAutomatically:
    // make this a setting, so they can be backed up
    private boolean readOnly;
    private boolean flushOnEachCommit;
    private boolean closed;
    private PageStore pageStore;
    private ObjectArray<InDoubtTransaction> inDoubtTransactions;


    /**
     * Create new transaction log object. This will not open or create files
     * yet.
     *
     * @param database the database
     * @param readOnly if the log should be opened in read-only mode
     * @param pageStore the page store
     */
    public LogSystem(Database database, boolean readOnly, PageStore pageStore) {
        this.database = database;
        this.pageStore = pageStore;
        this.readOnly = readOnly;
        closed = true;
    }

    /**
     * Get the list of in-doubt transactions.
     *
     * @return the list
     */
    public ObjectArray<InDoubtTransaction> getInDoubtTransactions() {
        if (pageStore != null) {
            return pageStore.getInDoubtTransactions();
        }
        return inDoubtTransactions;
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    public void prepareCommit(Session session, String transaction) throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            pageStore.prepareCommit(session, transaction);
            if (closed) {
                return;
            }
        }
    }

    /**
     * Commit the current transaction of the given session.
     *
     * @param session the session
     */
    public void commit(Session session) throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            pageStore.commit(session);
            session.setAllCommitted();
            if (closed) {
                return;
            }
            session.setAllCommitted();
        }
    }

    /**
     * Flush all pending changes to the transaction log files.
     */
    public void flush() throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            pageStore.flushLog();
            if (closed) {
                return;
            }
        }
    }

    /**
     * Enable or disable-flush-on-each-commit.
     *
     * @param b the new value
     */
    public void setFlushOnEachCommit(boolean b) {
        flushOnEachCommit = b;
    }

    /**
     * Check if flush-on-each-commit is enabled.
     *
     * @return true if it is
     */
    public boolean getFlushOnEachCommit() {
        return flushOnEachCommit;
    }

    /**
     * Get the write position.
     *
     * @return the write position
     */
    public String getWritePos() {
        return "" + pageStore.getWriteCountTotal();
    }

}
