/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.json.JSONBytesSource;
import org.h2.util.json.JSONItemType;
import org.h2.util.json.JSONStringSource;
import org.h2.util.json.JSONValidationTarget;
import org.h2.util.json.JSONValidationTargetWithUniqueKeys;
import org.h2.util.json.JSONValidationTargetWithoutUniqueKeys;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;

/**
 * IS JSON predicate.
 */
public final class IsJsonPredicate extends Condition {

    private Expression left;
    private final boolean not;
    private final boolean whenOperand;
    private final boolean withUniqueKeys;
    private final JSONItemType itemType;

    public IsJsonPredicate(Expression left, boolean not, boolean whenOperand, boolean withUniqueKeys,
            JSONItemType itemType) {
        this.left = left;
        this.whenOperand = whenOperand;
        this.not = not;
        this.withUniqueKeys = withUniqueKeys;
        this.itemType = itemType;
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        builder.append(" IS");
        if (not) {
            builder.append(" NOT");
        }
        builder.append(" JSON");
        switch (itemType) {
        case VALUE:
            break;
        case ARRAY:
            builder.append(" ARRAY");
            break;
        case OBJECT:
            builder.append(" OBJECT");
            break;
        case SCALAR:
            builder.append(" SCALAR");
            break;
        default:
            throw DbException.getInternalError("itemType=" + itemType);
        }
        if (withUniqueKeys) {
            builder.append(" WITH UNIQUE KEYS");
        }
        return builder;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (!whenOperand && left.isConstant()) {
            return ValueExpression.getBoolean(getValue(session));
        }
        return this;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(getValue(l));
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        if (left == ValueNull.INSTANCE) {
            return false;
        }
        return getValue(left);
    }

    private boolean getValue(Value left) {
        boolean result;
        switch (left.getValueType()) {
        case Value.VARBINARY:
        case Value.BINARY:
        case Value.BLOB: {
            byte[] bytes = left.getBytesNoCopy();
            JSONValidationTarget target = withUniqueKeys ? new JSONValidationTargetWithUniqueKeys()
                    : new JSONValidationTargetWithoutUniqueKeys();
            try {
                result = itemType.includes(JSONBytesSource.parse(bytes, target)) ^ not;
            } catch (RuntimeException ex) {
                result = not;
            }
            break;
        }
        case Value.JSON: {
            JSONItemType valueItemType = ((ValueJson) left).getItemType();
            if (!itemType.includes(valueItemType)) {
                result = not;
                break;
            } else if (!withUniqueKeys || valueItemType == JSONItemType.SCALAR) {
                result = !not;
                break;
            }
        }
        //$FALL-THROUGH$
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.CHAR:
        case Value.CLOB: {
            String string = left.getString();
            JSONValidationTarget target = withUniqueKeys ? new JSONValidationTargetWithUniqueKeys()
                    : new JSONValidationTargetWithoutUniqueKeys();
            try {
                result = itemType.includes(JSONStringSource.parse(string, target)) ^ not;
            } catch (RuntimeException ex) {
                result = not;
            }
            break;
        }
        default:
            result = not;
        }
        return result;
    }

    @Override
    public boolean isWhenConditionOperand() {
        return whenOperand;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new IsJsonPredicate(left, !not, false, withUniqueKeys, itemType);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor);
    }

    @Override
    public int getCost() {
        int cost = left.getCost();
        if (left.getType().getValueType() == Value.JSON && (!withUniqueKeys || itemType == JSONItemType.SCALAR)) {
            cost++;
        } else {
            cost += 10;
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        }
        throw new IndexOutOfBoundsException();
    }

}
