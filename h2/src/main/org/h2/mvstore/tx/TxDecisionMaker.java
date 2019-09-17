/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.MVMap;
import org.h2.value.VersionedValue;

/**
 * Class TxDecisionMaker is a base implementation of MVMap.DecisionMaker
 * to be used for TransactionMap modification.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
class TxDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
    /**
     * Map to decide upon
     */
    private final int            mapId;

    /**
     * Key for the map entry to decide upon
     */
    private final Object         key;

    /**
     * Value for the map entry
     */
    private final Object         value;

    /**
     * Transaction we are operating within
     */
    private final Transaction    transaction;

    /**
     * Id for the undo log entry created for this modification
     */
    private       long           undoKey;

    /**
     * Id of the last operation, we decided to {@link MVMap.Decision#REPEAT}.
     */
    private       long           lastOperationId;

    private       Transaction    blockingTransaction;
    private       MVMap.Decision decision;
    private       Object         lastCommittedValue;

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
            logAndDecideToPut(existingValue, existingValue == null ? null : existingValue.getCommittedValue());
        } else if (isCommitted(blockingId)) {
            // Condition above means that entry belongs to a committing transaction.
            // We assume that we are looking at the final value for this transaction,
            // and if it's not the case, then it will fail later,
            // because a tree root has definitely been changed.
            Object currentValue = existingValue.getCurrentValue();
            logAndDecideToPut(currentValue == null ? null : VersionedValueCommitted.getInstance(currentValue),
                                currentValue);
        } else if (getBlockingTransaction() != null) {
            // this entry comes from a different transaction, and this
            // transaction is not committed yet
            // should wait on blockingTransaction that was determined earlier
            decision = MVMap.Decision.ABORT;
        } else if (isRepeatedOperation(id)) {
            // There is no transaction with that id, and we've tried it just
            // before, but map root has not changed (which must be the case if
            // we just missed a closed transaction), therefore we came back here
            // again.
            // Now we assume it's a leftover after unclean shutdown (map update
            // was written but not undo log), and will effectively roll it back
            // (just assume committed value and overwrite).
            Object committedValue = existingValue.getCommittedValue();
            logAndDecideToPut(committedValue == null ? null : VersionedValueCommitted.getInstance(committedValue),
                                committedValue);
        } else {
            // transaction has been committed/rolled back and is closed by now, so
            // we can retry immediately and either that entry become committed
            // or we'll hit case above
            decision = MVMap.Decision.REPEAT;
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

    @SuppressWarnings("unchecked")
    @Override
    // always return value (ignores existingValue)
    public final VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
        return VersionedValueUncommitted.getInstance(undoKey, getNewValue(existingValue), lastCommittedValue);
    }

    /**
     * Get the new value.
     * This implementation always return the current value (ignores the parameter).
     *
     * @param existingValue the parameter value
     * @return the current value.
     */
    Object getNewValue(VersionedValue existingValue) {
        return value;
    }

    /**
     * Create undo log entry and record for future references {@link MVMap.Decision#PUT} decision
     * along with last known committed value
     *
     * @param valueToLog previous value to be logged
     * @param value last known committed value
     * @return {@link MVMap.Decision#PUT}
     */
    final MVMap.Decision logAndDecideToPut(VersionedValue valueToLog, Object value) {
        undoKey = transaction.log(mapId, key, valueToLog);
        lastCommittedValue = value;
        return setDecision(MVMap.Decision.PUT);
    }

    final MVMap.Decision getDecision() {
        return decision;
    }

    final Transaction getBlockingTransaction() {
        return blockingTransaction;
    }

    /**
     * Check whether specified transaction id belongs to "current" transaction
     * (transaction we are acting within).
     *
     * @param transactionId to check
     * @return true it it is "current" transaction's id, false otherwise
     */
    final boolean isThisTransaction(int transactionId) {
        return transactionId == transaction.transactionId;
    }

    /**
     * Determine whether specified id corresponds to a logically committed transaction.
     * In case of pending transaction, reference to actual Transaction object (if any)
     * is preserved for future use.
     *
     * @param transactionId to use
     * @return true if transaction should be considered as committed, false otherwise
     */
    final boolean isCommitted(int transactionId) {
        Transaction blockingTx;
        boolean result;
        TransactionStore store = transaction.store;
        do {
            blockingTx = store.getTransaction(transactionId);
            result = store.committingTransactions.get().get(transactionId);
        } while (blockingTx != store.getTransaction(transactionId));

        if (!result) {
            blockingTransaction = blockingTx;
        }
        return result;
    }

    /**
     * Store operation id provided, but before that, compare it against last stored one.
     * This is to prevent an infinite loop in case of uncommitted "leftover" entry
     * (one without a corresponding undo log entry, most likely as a result of unclean shutdown).
     *
     * @param id for the operation we decided to {@link MVMap.Decision#REPEAT}
     * @return true if the same as last operation id, false otherwise
     */
    final boolean isRepeatedOperation(long id) {
        if (id == lastOperationId) {
            return true;
        }
        lastOperationId = id;
        return false;
    }

    /**
     * Record for future references specified value as a decision that has been made.
     *
     * @param decision made
     * @return argument provided
     */
    final MVMap.Decision setDecision(MVMap.Decision decision) {
        return this.decision = decision;
    }

    @Override
    public final String toString() {
        return "txdm " + transaction.transactionId;
    }



    public static final class PutIfAbsentDecisionMaker extends TxDecisionMaker
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
                return logAndDecideToPut(null, null);
            } else {
                long id = existingValue.getOperationId();
                if (id == 0 // entry is a committed one
                            // or it came from the same transaction
                        || isThisTransaction(blockingId = TransactionStore.getTransactionId(id))) {
                    if(existingValue.getCurrentValue() != null) {
                        return setDecision(MVMap.Decision.ABORT);
                    }
                    return logAndDecideToPut(existingValue, existingValue.getCommittedValue());
                } else if (isCommitted(blockingId)) {
                    // entry belongs to a committing transaction
                    // and therefore will be committed soon
                    if(existingValue.getCurrentValue() != null) {
                        return setDecision(MVMap.Decision.ABORT);
                    }
                    return logAndDecideToPut(null, null);
                } else if (getBlockingTransaction() != null) {
                    // this entry comes from a different transaction, and this
                    // transaction is not committed yet
                    // should wait on blockingTransaction that was determined
                    // earlier and then try again
                    return setDecision(MVMap.Decision.ABORT);
                } else if (isRepeatedOperation(id)) {
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
                    return logAndDecideToPut(null, null);
                } else {
                    // transaction has been committed/rolled back and is closed
                    // by now, so we can retry immediately and either that entry
                    // become committed or we'll hit case above
                    return setDecision(MVMap.Decision.REPEAT);
                }
            }
        }
    }


    public static final class LockDecisionMaker extends TxDecisionMaker {

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

        @Override
        Object getNewValue(VersionedValue existingValue) {
            return existingValue == null ? null : existingValue.getCurrentValue();
        }
    }
}
