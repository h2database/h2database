/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Prepared;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * Represents the ROWNUM function.
 */
public final class Rownum extends Operation0 {

    private final Prepared prepared;

    private boolean singleRow;

    public Rownum(Prepared prepared) {
        if (prepared == null) {
            throw DbException.getInternalError();
        }
        this.prepared = prepared;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return ValueBigint.get(prepared.getCurrentRowNumber());
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_BIGINT;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return builder.append("ROWNUM()");
    }

    @Override
    public Expression optimize(SessionLocal session) {
        return singleRow ? ValueExpression.get(ValueBigint.get(1L)) : this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.EVALUATABLE:
            return false;
        case ExpressionVisitor.DECREMENT_QUERY_LEVEL:
            if (visitor.getQueryLevel() > 0) {
                singleRow = true;
            }
            //$FALL-THROUGH$
        default:
            return true;
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

}
