/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.BitSet;

import org.h2.mvstore.RootReference;

/**
 * Snapshot of the map root and committing transactions.
 */
final class Snapshot {

    /**
     * The root reference.
     */
    final RootReference root;

    /**
     * The committing transactions (see also TransactionStore.committingTransactions).
     */
    final BitSet committingTransactions;

    Snapshot(RootReference root, BitSet committingTransactions) {
        this.root = root;
        this.committingTransactions = committingTransactions;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + committingTransactions.hashCode();
        result = prime * result + root.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Snapshot)) {
            return false;
        }
        Snapshot other = (Snapshot) obj;
        return committingTransactions == other.committingTransactions && root == other.root;
    }

}
