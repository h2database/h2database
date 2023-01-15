/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

import org.h2.util.ByteStack;

/**
 * JSON validation target without unique keys.
 */
public final class JSONValidationTargetWithoutUniqueKeys extends JSONValidationTarget {

    private static final byte OBJECT = 1;

    private static final byte ARRAY = 2;

    private JSONItemType type;

    private final ByteStack stack;

    private boolean needSeparator;

    private boolean afterName;

    /**
     * Creates new instance of JSON validation target without unique keys.
     */
    public JSONValidationTargetWithoutUniqueKeys() {
        stack = new ByteStack();
    }

    @Override
    public void startObject() {
        beforeValue();
        afterName = false;
        stack.push(OBJECT);
    }

    @Override
    public void endObject() {
        if (afterName || stack.poll(-1) != OBJECT) {
            throw new IllegalStateException();
        }
        afterValue(JSONItemType.OBJECT);
    }

    @Override
    public void startArray() {
        beforeValue();
        afterName = false;
        stack.push(ARRAY);
    }

    @Override
    public void endArray() {
        if (stack.poll(-1) != ARRAY) {
            throw new IllegalStateException();
        }
        afterValue(JSONItemType.ARRAY);
    }

    @Override
    public void member(String name) {
        if (afterName || stack.peek(-1) != OBJECT) {
            throw new IllegalStateException();
        }
        afterName = true;
        beforeValue();
    }

    @Override
    public void valueNull() {
        beforeValue();
        afterValue(JSONItemType.SCALAR);
    }

    @Override
    public void valueFalse() {
        beforeValue();
        afterValue(JSONItemType.SCALAR);
    }

    @Override
    public void valueTrue() {
        beforeValue();
        afterValue(JSONItemType.SCALAR);
    }

    @Override
    public void valueNumber(BigDecimal number) {
        beforeValue();
        afterValue(JSONItemType.SCALAR);
    }

    @Override
    public void valueString(String string) {
        beforeValue();
        afterValue(JSONItemType.SCALAR);
    }

    private void beforeValue() {
        if (!afterName && stack.peek(-1) == OBJECT) {
            throw new IllegalStateException();
        }
        if (needSeparator) {
            if (stack.isEmpty()) {
                throw new IllegalStateException();
            }
            needSeparator = false;
        }
    }

    private void afterValue(JSONItemType type) {
        needSeparator = true;
        afterName = false;
        if (stack.isEmpty()) {
            this.type = type;
        }
    }

    @Override
    public boolean isPropertyExpected() {
        return !afterName && stack.peek(-1) == OBJECT;
    }

    @Override
    public boolean isValueSeparatorExpected() {
        return needSeparator;
    }

    @Override
    public JSONItemType getResult() {
        if (!stack.isEmpty() || type == null) {
            throw new IllegalStateException();
        }
        return type;
    }

}
