/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.mvstore.db.Store;
import org.h2.util.ParserUtil;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * Field reference.
 */
public final class FieldReference extends Operation1 {

    private final String fieldName;

    private int ordinal;

    public FieldReference(Expression arg, String fieldName) {
        super(arg);
        this.fieldName = fieldName;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return ParserUtil.quoteIdentifier(arg.getEnclosedSQL(builder, sqlFlags).append('.'), fieldName, sqlFlags);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = arg.getValue(session);
        if (l != ValueNull.INSTANCE) {
            return ((ValueRow) l).getList()[ordinal];
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        TypeInfo type = arg.getType();
        if (type.getValueType() != Value.ROW) {
            throw Store.getInvalidExpressionTypeException("ROW", arg);
        }
        int ordinal = 0;
        for (Entry<String, TypeInfo> entry : ((ExtTypeInfoRow) type.getExtTypeInfo()).getFields()) {
            if (fieldName.equals(entry.getKey())) {
                type = entry.getValue();
                this.type = type;
                this.ordinal = ordinal;
                if (arg.isConstant()) {
                    return TypedValueExpression.get(getValue(session), type);
                }
                return this;
            }
            ordinal++;
        }
        throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, fieldName);
    }

}
