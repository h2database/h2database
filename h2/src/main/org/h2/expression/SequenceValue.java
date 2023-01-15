/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Prepared;
import org.h2.engine.SessionLocal;
import org.h2.schema.Sequence;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Wraps a sequence when used in a statement.
 */
public final class SequenceValue extends Operation0 {

    private final Sequence sequence;

    private final boolean current;

    private final Prepared prepared;

    /**
     * Creates new instance of NEXT VALUE FOR expression.
     *
     * @param sequence
     *            the sequence
     * @param prepared
     *            the owner command, or {@code null}
     */
    public SequenceValue(Sequence sequence, Prepared prepared) {
        this.sequence = sequence;
        current = false;
        this.prepared = prepared;
    }

    /**
     * Creates new instance of CURRENT VALUE FOR expression.
     *
     * @param sequence
     *            the sequence
     */
    public SequenceValue(Sequence sequence) {
        this.sequence = sequence;
        current = true;
        prepared = null;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return current ? session.getCurrentValueFor(sequence) : session.getNextValueFor(sequence, prepared);
    }

    @Override
    public TypeInfo getType() {
        return sequence.getDataType();
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append(current ? "CURRENT" : "NEXT").append(" VALUE FOR ");
        return sequence.getSQL(builder, sqlFlags);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return false;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            visitor.addDataModificationId(sequence.getModificationId());
            return true;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(sequence);
            return true;
        case ExpressionVisitor.READONLY:
            return current;
        default:
            return true;
        }
    }

    @Override
    public int getCost() {
        return 1;
    }

}
