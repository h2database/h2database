/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.log.LogSystem;
import org.h2.message.Message;
import org.h2.result.LocalResult;

/**
 * Represents a transactional statement.
 */
public class TransactionCommand extends Prepared {

    /**
     * The type of a SET AUTOCOMMIT TRUE statement.
     */
    public static final int AUTOCOMMIT_TRUE = 1;

    /**
     * The type of a SET AUTOCOMMIT FALSE statement.
     */
    public static final int AUTOCOMMIT_FALSE = 2;

    /**
     * The type of a COMMIT statement.
     */
    public static final int COMMIT = 3;

    /**
     * The type of a ROLLBACK statement.
     */
    public static final int ROLLBACK = 4;

    /**
     * The type of a CHECKPOINT statement.
     */
    public static final int CHECKPOINT = 5;

    /**
     * The type of a SAVEPOINT statement.
     */
    public static final int SAVEPOINT = 6;

    /**
     * The type of a ROLLBACK TO SAVEPOINT statement.
     */
    public static final int ROLLBACK_TO_SAVEPOINT = 7;

    /**
     * The type of a CHECKPOINT SYNC statement.
     */
    public static final int CHECKPOINT_SYNC = 8;

    /**
     * The type of a PREPARE COMMIT statement.
     */
    public static final int PREPARE_COMMIT = 9;

    /**
     * The type of a COMMIT TRANSACTION statement.
     */
    public static final int COMMIT_TRANSACTION = 10;

    /**
     * The type of a ROLLBACK TRANSACTION statement.
     */
    public static final int ROLLBACK_TRANSACTION = 11;

    /**
     * The type of a SHUTDOWN statement.
     */
    public static final int SHUTDOWN = 12;

    /**
     * The type of a SHUTDOWN IMMEDIATELY statement.
     */
    public static final int SHUTDOWN_IMMEDIATELY = 13;

    /**
     * The type of a BEGIN {WORK|TRANSACTION} statement.
     */
    public static final int BEGIN = 14;

    private int type;
    private String savepointName;
    private String transactionName;

    public TransactionCommand(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setSavepointName(String name) {
        this.savepointName = name;
    }

    public int update() throws SQLException {
        switch (type) {
        case AUTOCOMMIT_TRUE:
            session.setAutoCommit(true);
            break;
        case AUTOCOMMIT_FALSE:
            session.setAutoCommit(false);
            break;
        case BEGIN:
            session.begin();
            break;
        case COMMIT:
            session.commit(false);
            break;
        case ROLLBACK:
            session.rollback();
            break;
        case CHECKPOINT:
            session.getUser().checkAdmin();
            session.getDatabase().checkpoint();
            break;
        case SAVEPOINT:
            session.addSavepoint(savepointName);
            break;
        case ROLLBACK_TO_SAVEPOINT:
            session.rollbackToSavepoint(savepointName);
            break;
        case CHECKPOINT_SYNC:
            session.getUser().checkAdmin();
            session.getDatabase().sync();
            break;
        case PREPARE_COMMIT:
            session.prepareCommit(transactionName);
            break;
        case COMMIT_TRANSACTION:
            session.getUser().checkAdmin();
            session.setPreparedTransaction(transactionName, true);
            break;
        case ROLLBACK_TRANSACTION:
            session.getUser().checkAdmin();
            session.setPreparedTransaction(transactionName, false);
            break;
        case SHUTDOWN_IMMEDIATELY:
            session.getUser().checkAdmin();
            session.getDatabase().shutdownImmediately();
            break;
        case SHUTDOWN: {
            session.getUser().checkAdmin();
            session.commit(false);
            // close the database, but don't update the persistent setting
            session.getDatabase().setCloseDelay(0);
            Database db = session.getDatabase();
            // throttle, to allow testing concurrent
            // execution of shutdown and query
            session.throttle();
            for (Session s : db.getSessions(false)) {
                if (db.isMultiThreaded()) {
                    synchronized (s) {
                        s.rollback();
                    }
                } else {
                    // if not multi-threaded, the session could already own
                    // the lock, which would result in a deadlock
                    // the other session can not concurrently do anything
                    // because the current session has locked the database
                    s.rollback();
                }
                if (s != session) {
                    s.close();
                }
            }
            LogSystem log = db.getLog();
            log.setDisabled(false);
            log.checkpoint();
            session.close();
            break;
        }
        default:
            Message.throwInternalError("type=" + type);
        }
        return 0;
    }

    public boolean isTransactional() {
        return true;
    }

    public boolean needRecompile() {
        return false;
    }

    public void setTransactionName(String string) {
        this.transactionName = string;
    }

    public LocalResult queryMeta() {
        return null;
    }

}
