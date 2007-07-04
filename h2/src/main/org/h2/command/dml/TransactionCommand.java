/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.LocalResult;


/**
 * @author Thomas
 */
public class TransactionCommand extends Prepared {
    public static final int AUTOCOMMIT_TRUE = 1;
    public static final int AUTOCOMMIT_FALSE = 2;
    public static final int COMMIT = 3;
    public static final int ROLLBACK = 4;
    public static final int CHECKPOINT = 5;
    public static final int SAVEPOINT = 6;
    public static final int ROLLBACK_TO_SAVEPOINT = 7;
    public static final int CHECKPOINT_SYNC = 8;
    public static final int PREPARE_COMMIT = 9;
    public static final int COMMIT_TRANSACTION = 10;
    public static final int ROLLBACK_TRANSACTION = 11;
    public static final int SHUTDOWN = 12;
    public static final int SHUTDOWN_IMMEDIATELY = 13;

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
        case COMMIT:
            session.commit(false);
            break;
        case ROLLBACK:
            session.rollback();
            break;
        case CHECKPOINT:
            session.getUser().checkAdmin();
            session.getDatabase().getLog().checkpoint();
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
            session.getDatabase().setPowerOffCount(1);
            try {
                session.getDatabase().checkPowerOff();
            } catch(SQLException e) {
                // ignore
            }
            break;
        case SHUTDOWN: {
            session.getUser().checkAdmin();
            session.commit(false);
            // close the database, but don't update the persistent setting
            session.getDatabase().setCloseDelay(0);
            Database db = session.getDatabase();
            Session[] sessions = db.getSessions();
            for(int i=0; i<sessions.length; i++) {
                Session s = sessions[i];
                synchronized(s) {
                    s.rollback();
                }
                if(s != session) {
                    s.close();
                }
            }
            db.getLog().checkpoint();
            session.close();
            break;
        }
        default:
            throw Message.getInternalError("type=" + type);
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
