/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.CursorPos;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.value.VersionedValue;

import java.util.BitSet;

import static org.h2.value.VersionedValue.NO_ENTRY_ID;
import static org.h2.value.VersionedValue.NO_OPERATION_ID;

/**
 * Class CommitDecisionMaker makes a decision during post-commit processing
 * about how to transform uncommitted map entry into committed one,
 * based on undo log information.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class CommitDecisionMaker<V> extends MVMap.DecisionMaker<VersionedValue<V>> {
    private final int transactionId;
    private long undoKey;
    private MVMap.Decision decision;

    private final BitSet entryIds;
    private final int pageEntryIds[];
    private int entryIdsCount;

    public CommitDecisionMaker(Transaction transaction, int maxKeysPerPage) {
        transactionId = transaction.getId();
        pageEntryIds = new int[maxKeysPerPage];
        entryIds = new BitSet((int)transaction.getLogId());
    }

    void setUndoKey(long undoKey) {
        this.undoKey = undoKey;
        reset();
    }

    @Override
    public void onPageReplaced() {
        for (int i = 0; i < entryIdsCount; i++) {
            entryIds.set(pageEntryIds[i]);
        }
        reset();
    }

    public boolean haveSeenEntry(int entryId) {
        return entryIds.get(entryId);
    }

    @Override
    public <K> CursorPos<K, VersionedValue<V>> decide(CursorPos<K, VersionedValue<V>> tip,
                                                        K key, VersionedValue<V> providedValue) {
        Page<K,VersionedValue<V>> p = tip.page;
        assert p.isLeaf();
        boolean update = false;
        long toRemove = 0L;
        for (int src = 0; src < p.getKeyCount(); src++) {
            VersionedValue<V> value = p.getValue(src);
            long operationId = value.getOperationId();
            if (operationId != NO_OPERATION_ID && TransactionStore.getTransactionId(operationId) == transactionId) {
                long entryId = value.getEntryId();
                assert entryId != NO_ENTRY_ID;
                assert !entryIds.get((int)entryId);
                pageEntryIds[entryIdsCount++] = (int)entryId;

                V currentValue = value.getCurrentValue();
                if (currentValue == null) {
                    toRemove |= 1L << src;
                } else {
                    update = true;
                }
            }
        }
        if (toRemove != 0L) {
            p = p.remove(toRemove);
            if (p.getKeyCount() == 0) {
                CursorPos<K, VersionedValue<V>> pos = tip.parent;
                if (pos != null) {
                    int keyCount;
                    int index;
                    do {
                        p = pos.page;
                        index = pos.index;
                        pos = pos.parent;
                        keyCount = p.getKeyCount();
                        // condition below should always be false, but older
                        // versions (up to 1.4.197) may create
                        // single-childed (with no keys) internal nodes,
                        // which we skip here
                    } while (keyCount == 0 && pos != null);

                    if (keyCount <= 1) {
                        if (keyCount == 1) {
                            assert index <= 1;
                            p = p.getChildPage(1 - index).copy();
                        } else {
                            // if root happens to be such single-childed
                            // (with no keys) internal node, then just
                            // replace it with empty leaf
                            p = Page.createEmptyLeaf(p.map);
                        }
                        return new CursorPos<>(p, 0, pos);
                    }
                    p = p.copy();
                    p.remove(index);
                }
                return new CursorPos<>(p, 0, pos);
            }
        } else if (update) {
            p = p.copy();
        } else {
            return tip;
        }
        if (update) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                VersionedValue<V> value = p.getValue(i);
                long operationId = value.getOperationId();
                if (operationId != NO_OPERATION_ID &&
                        TransactionStore.getTransactionId(operationId) == transactionId) {
                    V currentValue = value.getCurrentValue();
                    assert currentValue != null;
                    p.setValue(i, VersionedValueCommitted.getInstance(currentValue));
                }
            }
        }
        return new CursorPos<>(p, p.getMemory(), tip.parent);
    }

    @Override
    public MVMap.Decision decide(VersionedValue<V> existingValue, VersionedValue<V> providedValue) {
        assert decision == null;
        if (existingValue == null ||
            // map entry was treated as already committed, and then
            // it has been removed by another transaction (committed and closed by now)
            existingValue.getOperationId() != undoKey) {
            // this is not a final undo log entry for this key,
            // or map entry was treated as already committed and then
            // overwritten by another transaction
            // see TxDecisionMaker.decide()

            decision = MVMap.Decision.ABORT;
        } else /* this is final undo log entry for this key */ if (existingValue.getCurrentValue() == null) {
            decision = MVMap.Decision.REMOVE;
        } else {
            decision = MVMap.Decision.PUT;
        }
        return decision;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends VersionedValue<V>> T selectValue(T existingValue, T providedValue) {
        assert decision == MVMap.Decision.PUT;
        assert existingValue != null;
        return (T) VersionedValueCommitted.getInstance(existingValue.getCurrentValue());
    }

    @Override
    public void reset() {
        decision = null;
        entryIdsCount = 0;
    }

    @Override
    public String toString() {
        return "commit " + TransactionStore.getTransactionId(undoKey);
    }
}
