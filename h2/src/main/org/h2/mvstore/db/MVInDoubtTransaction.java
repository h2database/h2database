/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.mvstore.MVStore;
import org.h2.mvstore.tx.Transaction;
import org.h2.store.InDoubtTransaction;

/**
 * An in-doubt transaction.
 */
final class MVInDoubtTransaction implements InDoubtTransaction {

    private final MVStore store;
    private final Transaction transaction;
    private int state = InDoubtTransaction.IN_DOUBT;

    MVInDoubtTransaction(MVStore store, Transaction transaction) {
        this.store = store;
        this.transaction = transaction;
    }

    @Override
    public void setState(int state) {
        if (state == InDoubtTransaction.COMMIT) {
            transaction.commit();
        } else {
            transaction.rollback();
        }
        store.commit();
        this.state = state;
    }

    @Override
    public int getState() {
        return state;
    }

    @Override
    public String getTransactionName() {
        return transaction.getName();
    }

}
