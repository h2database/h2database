/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import org.h2.message.DbException;

/**
 * Represents an in-doubt transaction (a transaction in the prepare phase).
 */
public interface InDoubtTransaction {

    /**
     * The transaction state meaning this transaction is not committed yet, but
     * also not rolled back (in-doubt).
     */
    int IN_DOUBT = 0;

    /**
     * The transaction state meaning this transaction is committed.
     */
    int COMMIT = 1;

    /**
     * The transaction state meaning this transaction is rolled back.
     */
    int ROLLBACK = 2;

    /**
     * Change the state of this transaction.
     * This will also update the transaction log.
     *
     * @param state the new state
     */
    void setState(int state);

    /**
     * Get the state of this transaction.
     *
     * @return the transaction state
     */
    int getState();

    /**
     * Get the state of this transaction as a text.
     *
     * @return the transaction state text
     */
    default String getStateDescription() {
        int state = getState();
        switch (state) {
        case 0:
            return "IN_DOUBT";
        case 1:
            return "COMMIT";
        case 2:
            return "ROLLBACK";
        default:
            throw DbException.getInternalError("state=" + state);
        }
    }

    /**
     * Get the name of the transaction.
     *
     * @return the transaction name
     */
    String getTransactionName();
}
