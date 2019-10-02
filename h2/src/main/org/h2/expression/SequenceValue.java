/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Sequence;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Wraps a sequence when used in a statement.
 */
public class SequenceValue extends Expression {

    private final Sequence sequence;

    private final boolean current;

    public SequenceValue(Sequence sequence, boolean current) {
        this.sequence = sequence;
        this.current = current;
    }

    @Override
    public Value getValue(Session session) {
        return current ? session.getCurrentValueFor(sequence) : sequence.getNext(session);
    }

    @Override
    public TypeInfo getType() {
        return sequence.getDatabase().getMode().decimalSequences ? TypeInfo.TYPE_DECIMAL : TypeInfo.TYPE_LONG;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        // nothing to do
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append(current ? "CURRENT" : "NEXT").append(" VALUE FOR ");
        return sequence.getSQL(builder, alwaysQuote);
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_COLUMNS1:
        case ExpressionVisitor.GET_COLUMNS2:
            return true;
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
            throw DbException.throwInternalError("type="+visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 1;
    }

}
