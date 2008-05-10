/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import java.sql.SQLException;

import org.h2.message.Message;

/**
 * Represents an in-doubt transaction (a transaction in the prepare phase).
 */
public class InDoubtTransaction {

    public static final int IN_DOUBT = 0, COMMIT = 1, ROLLBACK = 2;

    // TODO 2-phase-commit: document sql statements and metadata table

    private LogFile log;
    private int sessionId;
    private int pos;
    private String transaction;
    private int blocks;
    private int state;

    public InDoubtTransaction(LogFile log, int sessionId, int pos, String transaction, int blocks) {
        this.log = log;
        this.sessionId = sessionId;
        this.pos = pos;
        this.transaction = transaction;
        this.blocks = blocks;
        this.state = IN_DOUBT;
    }

    public void setState(int state) throws SQLException {
        switch(state) {
        case COMMIT:
            log.updatePreparedCommit(true, pos, sessionId, blocks);
            break;
        case ROLLBACK:
            log.updatePreparedCommit(false, pos, sessionId, blocks);
            break;
        default:
            throw Message.getInternalError("state="+state);
        }
        this.state = state;
    }

    public String getState() {
        switch(state) {
        case IN_DOUBT:
            return "IN_DOUBT";
        case COMMIT:
            return "COMMIT";
        case ROLLBACK:
            return "ROLLBACK";
        default:
            throw Message.getInternalError("state="+state);
        }
    }

    public int getPos() {
        return pos;
    }

    public int getSessionId() {
        return sessionId;
    }

    public String getTransaction() {
        return transaction;
    }

}
