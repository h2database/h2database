/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import org.h2.message.DbException;

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
    private final int sessionId;
    private final int pos;
    private final String transaction;
    private int state;

    /**
     * Create a new in-doubt transaction info object.
     *
     * @param store the page store
     * @param sessionId the session id
     * @param pos the position
     * @param transaction the transaction name
     */
    public InDoubtTransaction(PageStore store, int sessionId, int pos, String transaction) {
        this.store = store;
        this.sessionId = sessionId;
        this.pos = pos;
        this.transaction = transaction;
        this.state = IN_DOUBT;
    }

    /**
     * Change the state of this transaction.
     * This will also update the transaction log.
     *
     * @param state the new state
     */
    public void setState(int state) {
        switch(state) {
        case COMMIT:
            store.setInDoubtTransactionState(sessionId, pos, true);
            break;
        case ROLLBACK:
            store.setInDoubtTransactionState(sessionId, pos, false);
            break;
        default:
            DbException.throwInternalError("state="+state);
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
            throw DbException.throwInternalError("state="+state);
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
