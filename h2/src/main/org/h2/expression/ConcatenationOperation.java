/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Arrays;

import org.h2.engine.SessionLocal;
import org.h2.expression.function.CastSpecification;
import org.h2.expression.function.ConcatFunction;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * Character string concatenation as in {@code 'Hello' || 'World'}, binary
 * string concatenation as in {@code X'01' || X'AB'} or an array concatenation
 * as in {@code ARRAY[1, 2] || 3}.
 */
public final class ConcatenationOperation extends OperationN {

    public ConcatenationOperation() {
        super(new Expression[4]);
    }

    public ConcatenationOperation(Expression op1, Expression op2) {
        super(new Expression[] { op1, op2 });
        argsCount = 2;
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        for (int i = 0, l = args.length; i < l; i++) {
            if (i > 0) {
                builder.append(" || ");
            }
            args[i].getSQL(builder, sqlFlags, AUTO_PARENTHESES);
        }
        return builder;
    }

    @Override
    public Value getValue(SessionLocal session) {
        int l = args.length;
        if (l == 2) {
            Value v1 = args[0].getValue(session);
            v1 = v1.convertTo(type, session);
            if (v1 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            Value v2 = args[1].getValue(session);
            v2 = v2.convertTo(type, session);
            if (v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return getValue(session, v1, v2);
        }
        return getValue(session, l);
    }

    private Value getValue(SessionLocal session, Value l, Value r) {
        int valueType = type.getValueType();
        if (valueType == Value.VARCHAR) {
            String s1 = l.getString(), s2 = r.getString();
            return ValueVarchar.get(new StringBuilder(s1.length() + s2.length()).append(s1).append(s2).toString());
        } else if (valueType == Value.VARBINARY) {
            byte[] leftBytes = l.getBytesNoCopy(), rightBytes = r.getBytesNoCopy();
            int leftLength = leftBytes.length, rightLength = rightBytes.length;
            byte[] bytes = Arrays.copyOf(leftBytes, leftLength + rightLength);
            System.arraycopy(rightBytes, 0, bytes, leftLength, rightLength);
            return ValueVarbinary.getNoCopy(bytes);
        } else {
            Value[] leftValues = ((ValueArray) l).getList(), rightValues = ((ValueArray) r).getList();
            int leftLength = leftValues.length, rightLength = rightValues.length;
            Value[] values = Arrays.copyOf(leftValues, leftLength + rightLength);
            System.arraycopy(rightValues, 0, values, leftLength, rightLength);
            return ValueArray.get((TypeInfo) type.getExtTypeInfo(), values, session);
        }
    }

    private Value getValue(SessionLocal session, int l) {
        Value[] values = new Value[l];
        for (int i = 0; i < l; i++) {
            Value v = args[i].getValue(session).convertTo(type, session);
            if (v == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            values[i] = v;
        }
        int valueType = type.getValueType();
        if (valueType == Value.VARCHAR) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < l; i++) {
                builder.append(values[i].getString());
            }
            return ValueVarchar.get(builder.toString(), session);
        } else if (valueType == Value.VARBINARY) {
            int totalLength = 0;
            for (int i = 0; i < l; i++) {
                totalLength += values[i].getBytesNoCopy().length;
            }
            byte[] v = new byte[totalLength];
            int offset = 0;
            for (int i = 0; i < l; i++) {
                byte[] a = values[i].getBytesNoCopy();
                int length = a.length;
                System.arraycopy(a, 0, v, offset, length);
                offset += length;
            }
            return ValueVarbinary.getNoCopy(v);
        } else {
            int totalLength = 0;
            for (int i = 0; i < l; i++) {
                totalLength += ((ValueArray) values[i]).getList().length;
            }
            Value[] v = new Value[totalLength];
            int offset = 0;
            for (int i = 0; i < l; i++) {
                Value[] a = ((ValueArray) values[i]).getList();
                int length = a.length;
                System.arraycopy(a, 0, v, offset, length);
                offset += length;
            }
            return ValueArray.get((TypeInfo) type.getExtTypeInfo(), v, session);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        determineType(session);
        inlineArguments();
        if (type.getValueType() == Value.VARCHAR && session.getMode().treatEmptyStringsAsNull) {
            return new ConcatFunction(ConcatFunction.CONCAT, args).optimize(session);
        }
        int l = args.length;
        boolean allConst = true, anyConst = false;
        for (int i = 0; i < l; i++) {
            if (args[i].isConstant()) {
                anyConst = true;
            } else {
                allConst = false;
            }
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        if (anyConst) {
            int offset = 0;
            for (int i = 0; i < l; i++) {
                Expression arg1 = args[i];
                if (arg1.isConstant()) {
                    Value v1 = arg1.getValue(session).convertTo(type, session);
                    if (v1 == ValueNull.INSTANCE) {
                        return TypedValueExpression.get(ValueNull.INSTANCE, type);
                    }
                    if (isEmpty(v1)) {
                        continue;
                    }
                    for (Expression arg2; i + 1 < l && (arg2 = args[i + 1]).isConstant(); i++) {
                        Value v2 = arg2.getValue(session).convertTo(type, session);
                        if (v2 == ValueNull.INSTANCE) {
                            return TypedValueExpression.get(ValueNull.INSTANCE, type);
                        }
                        if (!isEmpty(v2)) {
                            v1 = getValue(session, v1, v2);
                        }
                    }
                    arg1 = ValueExpression.get(v1);
                }
                args[offset++] = arg1;
            }
            if (offset == 1) {
                Expression arg = args[0];
                TypeInfo argType = arg.getType();
                if (TypeInfo.areSameTypes(type, argType)) {
                    return arg;
                }
                return new CastSpecification(arg, type);
            }
            argsCount = offset;
            doneWithParameters();
        }
        return this;
    }

    private void determineType(SessionLocal session) {
        int l = args.length;
        boolean anyArray = false, allBinary = true, allCharacter = true;
        for (int i = 0; i < l; i++) {
            Expression arg = args[i].optimize(session);
            args[i] = arg;
            int t = arg.getType().getValueType();
            if (t == Value.ARRAY) {
                anyArray = true;
                allBinary = allCharacter = false;
            } else if (t == Value.NULL) {
                // Ignore NULL literals
            } else if (DataType.isBinaryStringType(t)) {
                allCharacter = false;
            } else if (DataType.isCharacterStringType(t)) {
                allBinary = false;
            } else {
                allBinary = allCharacter = false;
            }
        }
        if (anyArray) {
            type = TypeInfo.getTypeInfo(Value.ARRAY, -1, 0, TypeInfo.getHigherType(args).getExtTypeInfo());
        } else if (allBinary) {
            long precision = getPrecision(0);
            for (int i = 1; i < l; i++) {
                precision = DataType.addPrecision(precision, getPrecision(i));
            }
            type = TypeInfo.getTypeInfo(Value.VARBINARY, precision, 0, null);
        } else if (allCharacter) {
            long precision = getPrecision(0);
            for (int i = 1; i < l; i++) {
                precision = DataType.addPrecision(precision, getPrecision(i));
            }
            type = TypeInfo.getTypeInfo(Value.VARCHAR, precision, 0, null);
        } else {
            type = TypeInfo.TYPE_VARCHAR;
        }
    }

    private long getPrecision(int i) {
        TypeInfo t = args[i].getType();
        return t.getValueType() != Value.NULL ? t.getPrecision() : 0L;
    }

    private void inlineArguments() {
        int valueType = type.getValueType();
        int l = args.length;
        int count = l;
        for (int i = 0; i < l; i++) {
            Expression arg = args[i];
            if (arg instanceof ConcatenationOperation && arg.getType().getValueType() == valueType) {
                count += arg.getSubexpressionCount() - 1;
            }
        }
        if (count > l) {
            Expression[] newArguments = new Expression[count];
            for (int i = 0, offset = 0; i < l; i++) {
                Expression arg = args[i];
                if (arg instanceof ConcatenationOperation && arg.getType().getValueType() == valueType) {
                    ConcatenationOperation c = (ConcatenationOperation) arg;
                    Expression[] innerArgs = c.args;
                    int innerLength = innerArgs.length;
                    System.arraycopy(innerArgs, 0, newArguments, offset, innerLength);
                    offset += innerLength;
                } else {
                    newArguments[offset++] = arg;
                }
            }
            args = newArguments;
            argsCount = count;
        }
    }

    private static boolean isEmpty(Value v) {
        int valueType = v.getValueType();
        if (valueType == Value.VARCHAR) {
            return v.getString().isEmpty();
        } else if (valueType == Value.VARBINARY) {
            return v.getBytesNoCopy().length == 0;
        } else {
            return ((ValueArray) v).getList().length == 0;
        }
    }

}
