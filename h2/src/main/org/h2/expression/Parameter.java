/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.condition.Comparison;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * A parameter of a prepared statement.
 */
public final class Parameter extends Operation0 implements ParameterInterface {

    /**
     * Returns the maximum 1-based index.
     *
     * @param parameters
     *            parameters
     * @return the maximum 1-based index, or {@code -1}
     */
    public static int getMaxIndex(ArrayList<Parameter> parameters) {
        int result = 0;
        for (Parameter p : parameters) {
            if (p != null) {
                int index = p.getIndex() + 1;
                if (index > result) {
                    result = index;
                }
            }
        }
        return result;
    }

    private Value value;
    private Column column;
    private final int index;

    public Parameter(int index) {
        this.index = index;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return builder.append('?').append(index + 1);
    }

    @Override
    public void setValue(Value v, boolean closeOld) {
        // don't need to close the old value as temporary files are anyway
        // removed
        this.value = v;
    }

    public void setValue(Value v) {
        this.value = v;
    }

    @Override
    public Value getParamValue() {
        if (value == null) {
            // to allow parameters in function tables
            return ValueNull.INSTANCE;
        }
        return value;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return getParamValue();
    }

    @Override
    public TypeInfo getType() {
        if (value != null) {
            return value.getType();
        }
        if (column != null) {
            return column.getType();
        }
        return TypeInfo.TYPE_UNKNOWN;
    }

    @Override
    public void checkSet() {
        if (value == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        if (session.getDatabase().getMode().treatEmptyStringsAsNull) {
            if (value instanceof ValueVarchar && value.getString().isEmpty()) {
                value = ValueNull.INSTANCE;
            }
        }
        return this;
    }

    @Override
    public boolean isValueSet() {
        return value != null;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.INDEPENDENT:
            return value != null;
        default:
            return true;
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        return new Comparison(Comparison.EQUAL, this, ValueExpression.FALSE, false);
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public int getIndex() {
        return index;
    }

}
