/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.store.PageStore;

/**
 * Represents an in-doubt transaction (a transaction in the prepare phase).
 */
public class InDoubtTransaction {

    /**
     * The transaction state meaning this transaction is not committed yet, but
     * also not rolled back (in-doubt).
     */
    public static final int IN_DOUBT = 0;

    /**
     * The transaction state meaning this transaction is committed.
     */
    public static final int COMMIT = 1;

    /**
     * The transaction state meaning this transaction is rolled back.
     */
    public static final int ROLLBACK = 2;

    // TODO 2-phase-commit: document sql statements and metadata table

    private final PageStore store;
    private final LogFile log;
    private final int sessionId;
    private final int pos;
    private final String transaction;
    private final int blocks;
    private int state;

    /**
     * Create a new in-doubt transaction info object.
     *
     * @param store the page store
     * @param log the log file
     * @param sessionId the session id
     * @param pos the position
     * @param transaction the transaction name
     * @param blocks the number of blocks the 'prepare commit' entry occupies
     */
    public InDoubtTransaction(PageStore store, LogFile log, int sessionId, int pos, String transaction, int blocks) {
        this.store = store;
        this.log = log;
        this.sessionId = sessionId;
        this.pos = pos;
        this.transaction = transaction;
        this.blocks = blocks;
        this.state = IN_DOUBT;
    }

    /**
     * Change the state of this transaction.
     * This will also update the log file.
     *
     * @param state the new state
     */
    public void setState(int state) throws SQLException {
        switch(state) {
        case COMMIT:
            if (store != null) {
                store.setInDoubtTransactionState(sessionId, pos, true);
            } else {
                log.updatePreparedCommit(true, pos, sessionId, blocks);
            }
            break;
        case ROLLBACK:
            if (store != null) {
                store.setInDoubtTransactionState(sessionId, pos, false);
            } else {
                log.updatePreparedCommit(false, pos, sessionId, blocks);
            }
            break;
        default:
            Message.throwInternalError("state="+state);
        }
        this.state = state;
    }

    /**
     * Get the state of this transaction as a text.
     *
     * @return the transaction state text
     */
    public String getState() {
        switch(state) {
        case IN_DOUBT:
            return "IN_DOUBT";
        case COMMIT:
            return "COMMIT";
        case ROLLBACK:
            return "ROLLBACK";
        default:
            throw Message.throwInternalError("state="+state);
        }
    }

    /**
     * Get the name of the transaction.
     *
     * @return the transaction name
     */
    public String getTransaction() {
        return transaction;
    }

}
