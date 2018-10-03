/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.MVMap;

/**
 * Class TxDecisionMaker.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
abstract class TxDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
    private final int            mapId;
    private final Object         key;
    final Object                 value;
    private final Transaction    transaction;
    long                         undoKey;
    protected     long           lastOperationId;
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
                isThisTransaction(blockingId = TransactionStore.getTransactionId(id))) {
            logIt(existingValue);
            decision = MVMap.Decision.PUT;
        } else if (isCommitted(blockingId)) {
            // Condition above means that entry belongs to a committing transaction.
            // We assume that we are looking at the final value for this transaction,
            // and if it's not the case, then it will fail later,
            // because a tree root has definitely been changed.
            logIt(existingValue.value == null ? null : VersionedValue.getInstance(existingValue.value));
            decision = MVMap.Decision.PUT;
        } else if (fetchTransaction(blockingId) != null) {
            // this entry comes from a different transaction, and this
            // transaction is not committed yet
            // should wait on blockingTransaction that was determined earlier
            decision = MVMap.Decision.ABORT;
        } else if (id == lastOperationId) {
            // There is no transaction with that id, and we've tried it just
            // before, but map root has not changed (which must be the case if
            // we just missed a closed transaction), therefore we came back here
            // again.
            // Now we assume it's a leftover after unclean shutdown (map update
            // was written but not undo log), and will effectively roll it back
            // (just assume committed value and overwrite).
            Object committedValue = existingValue.getCommittedValue();
            logIt(committedValue == null ? null : VersionedValue.getInstance(committedValue));
            decision = MVMap.Decision.PUT;
        } else {
            // transaction has been committed/rolled back and is closed by now, so
            // we can retry immediately and either that entry become committed
            // or we'll hit case above
            decision = MVMap.Decision.REPEAT;
            lastOperationId = id;
        }
        return decision;
    }

    @Override
    public final void reset() {
        if (decision != MVMap.Decision.REPEAT) {
            lastOperationId = 0;
            if (decision == MVMap.Decision.PUT) {
                // positive decision has been made already and undo record created,
                // but map was updated afterwards and undo record deletion required
                transaction.logUndo();
            }
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
            return VersionedValue.getInstance(undoKey, value,
                                                existingValue == null ? null : existingValue.getCommittedValue());
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
                        || isThisTransaction(blockingId = TransactionStore.getTransactionId(id))) {
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
                } else if (fetchTransaction(blockingId) != null) {
                    // this entry comes from a different transaction, and this
                    // transaction is not committed yet
                    // should wait on blockingTransaction that was determined
                    // earlier and then try again
                    return setDecision(MVMap.Decision.ABORT);
                } else if (id == lastOperationId) {
                    // There is no transaction with that id, and we've tried it
                    // just before, but map root has not changed (which must be
                    // the case if we just missed a closed transaction),
                    // therefore we came back here again.
                    // Now we assume it's a leftover after unclean shutdown (map
                    // update was written but not undo log), and will
                    // effectively roll it back (just assume committed value and
                    // overwrite).
                    Object committedValue = existingValue.getCommittedValue();
                    if (committedValue != null) {
                        return setDecision(MVMap.Decision.ABORT);
                    }
                    logIt(null);
                    return setDecision(MVMap.Decision.PUT);
                } else {
                    // transaction has been committed/rolled back and is closed
                    // by now, so we can retry immediately and either that entry
                    // become committed or we'll hit case above
                    lastOperationId = id;
                    return setDecision(MVMap.Decision.REPEAT);
                }
            }
        }
    }


    public static final class LockDecisionMaker extends TxDecisionMaker
    {
        LockDecisionMaker(int mapId, Object key, Transaction transaction) {
            super(mapId, key, null, transaction);
        }

        @Override
        public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
            MVMap.Decision decision = super.decide(existingValue, providedValue);
            if (existingValue == null) {
                assert decision == MVMap.Decision.PUT;
                decision = setDecision(MVMap.Decision.REMOVE);
            }
            return decision;
        }

        @SuppressWarnings("unchecked")
        @Override
        public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
            return VersionedValue.getInstance(undoKey,
                    existingValue == null ? null : existingValue.value,
                    existingValue == null ? null : existingValue.getCommittedValue());
        }
    }
}
