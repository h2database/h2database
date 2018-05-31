/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.MVMap;
import static org.h2.mvstore.tx.TransactionStore.getTransactionId;

/**
 * Class TxDecisionMaker.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class TxDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
    private final int            mapId;
    private final Object         key;
            final Object         value;
    private final Transaction    transaction;
                  long           undoKey;
    private       Transaction    blockingTransaction;
    private       MVMap.Decision decision;

    TxDecisionMaker(int mapId, Object key, Object value, Transaction transaction) {
        this.mapId = mapId;
        this.key = key;
        this.value = value;
        this.transaction = transaction;
    }

    @Override
    public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
        assert decision == null;
        long id;
        int blockingId;
        // if map does not have that entry yet
        if (existingValue == null ||
                // or entry is a committed one
                (id = existingValue.getOperationId()) == 0 ||
                // or it came from the same transaction
                isThisTransaction(blockingId = getTransactionId(id))) {
            logIt(existingValue);
            decision = MVMap.Decision.PUT;
        } else if (isCommitted(blockingId)) {
            // Condition above means that entry belongs to a committing transaction.
            // We assume that we are looking at the final value for this transaction,
            // and if it's not the case, then it will fail later,
            // because a tree root has definitely been changed.
            logIt(existingValue.value == null ? null : new VersionedValue(0L, existingValue.value));
            decision = MVMap.Decision.PUT;
        } else if(fetchTransaction(blockingId) == null) {
            // condition above means transaction has been committed/rplled back and closed by now
            decision = MVMap.Decision.REPEAT;
        } else {
            // this entry comes from a different transaction, and this transaction is not committed yet
            // should wait on blockingTransaction that was determined earlier
            decision = MVMap.Decision.ABORT;
        }
        return decision;
    }

    @Override
    public final void reset() {
        if (decision != null && decision != MVMap.Decision.ABORT && decision != MVMap.Decision.REPEAT) {
            // positive decision has been made already and undo record created,
            // but map was updated afterwards and undo record deletion required
            transaction.logUndo();
        }
        blockingTransaction = null;
        decision = null;
    }

    public final MVMap.Decision getDecision() {
        return decision;
    }

    final Transaction getBlockingTransaction() {
        return blockingTransaction;
    }

    final void logIt(VersionedValue value) {
        undoKey = transaction.log(mapId, key, value);
    }

    final boolean isThisTransaction(int transactionId) {
        return transactionId == transaction.transactionId;
    }

    final boolean isCommitted(int transactionId) {
        return transaction.store.committingTransactions.get().get(transactionId);
    }

    final Transaction fetchTransaction(int transactionId) {
        return (blockingTransaction = transaction.store.getTransaction(transactionId));
    }

    final MVMap.Decision setDecision(MVMap.Decision d) {
        return decision = d;
    }

    @Override
    public final String toString() {
        return "txdm " + transaction.transactionId;
    }


    public static class PutDecisionMaker extends TxDecisionMaker
    {
        PutDecisionMaker(int mapId, Object key, Object value, Transaction transaction) {
            super(mapId, key, value, transaction);
        }

        @SuppressWarnings("unchecked")
        @Override
        public final VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
            return new VersionedValue(undoKey, value);
        }
    }


    public static final class PutIfAbsentDecisionMaker extends PutDecisionMaker
    {
        PutIfAbsentDecisionMaker(int mapId, Object key, Object value, Transaction transaction) {
            super(mapId, key, value, transaction);
        }

        @Override
        public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
            assert getDecision() == null;
            int blockingId;
            // if map does not have that entry yet
            if (existingValue == null) {
                logIt(null);
                return setDecision(MVMap.Decision.PUT);
            } else {
                long id = existingValue.getOperationId();
                if (id == 0 // entry is a committed one
                            // or it came from the same transaction
                        || isThisTransaction(blockingId = getTransactionId(id))) {
                    if(existingValue.value != null) {
                        return setDecision(MVMap.Decision.ABORT);
                    }
                    logIt(existingValue);
                    return setDecision(MVMap.Decision.PUT);
                } else if (isCommitted(blockingId) && existingValue.value == null) {
                    // entry belongs to a committing transaction
                    // and therefore will be committed soon
                    logIt(null);
                    return setDecision(MVMap.Decision.PUT);
                } else if(fetchTransaction(blockingId) == null) {
                    // map already has specified key from uncommitted
                    // at the time transaction, which is closed by now
                    // we can retry right away
                    return setDecision(MVMap.Decision.REPEAT);
                } else {
                    // map already has specified key from uncommitted transaction
                    // we need to wait for it to close and then try again
                    return setDecision(MVMap.Decision.ABORT);
                }
            }
        }
    }


    public static final class LockDecisionMaker extends TxDecisionMaker
    {
        LockDecisionMaker(int mapId, Object key, Transaction transaction) {
            super(mapId, key, null, transaction);
        }

        @SuppressWarnings("unchecked")
        @Override
        public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
            return new VersionedValue(undoKey, existingValue == null ? null : existingValue.value);
        }
    }
}
