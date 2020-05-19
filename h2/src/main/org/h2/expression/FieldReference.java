/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Iterator;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * Field reference.
 */
public class FieldReference extends Operation1 {

    private final String fieldName;

    private int ordinal;

    public FieldReference(Expression arg, String fieldName) {
        super(arg);
        this.fieldName = fieldName;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return Parser.quoteIdentifier(arg.getSQL(builder.append("(("), sqlFlags).append(")."), fieldName, sqlFlags)
                .append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value l = arg.getValue(session);
        if (l != ValueNull.INSTANCE) {
            return ((ValueRow) l).getList()[ordinal];
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(Session session) {
        arg = arg.optimize(session);
        TypeInfo type = arg.getType();
        if (type.getValueType() != Value.ROW) {
            throw DbException.getInvalidValueException("ROW", type.getTraceSQL());
        }
        int ordinal;
        ExtTypeInfoRow ext = (ExtTypeInfoRow) type.getExtTypeInfo();
        find: if (ext != null) {
            ordinal = 0;
            Iterator<Entry<String, TypeInfo>> iter = ext.getFields().entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, TypeInfo> entry = iter.next();
                if (fieldName.equals(entry.getKey())) {
                    type = entry.getValue();
                    break find;
                }
                ordinal++;
            }
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, fieldName);
        } else {
            if (fieldName.length() < 2 && fieldName.charAt(0) != 'C') {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, fieldName);
            }
            try {
                ordinal = StringUtils.parseUInt31(fieldName, 1, fieldName.length());
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, fieldName);
            }
            if (ordinal < 1) {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, fieldName);
            }
            ordinal--;
        }
        this.type = type;
        this.ordinal = ordinal;
        if (arg.isConstant()) {
            return TypedValueExpression.get(getValue(session), type);
        }
        return this;
    }

}
