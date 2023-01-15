/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * JSON validation target with unique keys.
 */
public final class JSONValidationTargetWithUniqueKeys extends JSONValidationTarget {

    private final ArrayDeque<Object> stack;

    private final ArrayDeque<String> names;

    private boolean needSeparator;

    private String memberName;

    private JSONItemType type;

    /**
     * Creates new instance of JSON validation target with unique keys.
     */
    public JSONValidationTargetWithUniqueKeys() {
        stack = new ArrayDeque<>();
        names = new ArrayDeque<>();
    }

    @Override
    public void startObject() {
        beforeValue();
        names.push(memberName != null ? memberName : "");
        memberName = null;
        stack.push(new HashSet<>());
    }

    @Override
    public void endObject() {
        if (memberName != null) {
            throw new IllegalStateException();
        }
        if (!(stack.poll() instanceof HashSet)) {
            throw new IllegalStateException();
        }
        memberName = names.pop();
        afterValue(JSONItemType.OBJECT);
    }

    @Override
    public void startArray() {
        beforeValue();
        names.push(memberName != null ? memberName : "");
        memberName = null;
        stack.push(Collections.emptyList());
    }

    @Override
    public void endArray() {
        if (!(stack.poll() instanceof List)) {
            throw new IllegalStateException();
        }
        memberName = names.pop();
        afterValue(JSONItemType.ARRAY);
    }

    @Override
    public void member(String name) {
        if (memberName != null || !(stack.peek() instanceof HashSet)) {
            throw new IllegalStateException();
        }
        memberName = name;
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
        if (memberName == null && stack.peek() instanceof HashSet) {
            throw new IllegalStateException();
        }
        if (needSeparator) {
            if (stack.isEmpty()) {
                throw new IllegalStateException();
            }
            needSeparator = false;
        }
    }

    @SuppressWarnings("unchecked")
    private void afterValue(JSONItemType type) {
        Object parent = stack.peek();
        if (parent == null) {
            this.type = type;
        } else if (parent instanceof HashSet) {
            if (!((HashSet<String>) parent).add(memberName)) {
                throw new IllegalStateException();
            }
        }
        needSeparator = true;
        memberName = null;
    }

    @Override
    public boolean isPropertyExpected() {
        return memberName == null && stack.peek() instanceof HashSet;
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
