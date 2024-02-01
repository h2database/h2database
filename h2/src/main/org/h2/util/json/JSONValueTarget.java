/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;
import java.util.ArrayDeque;

/**
 * JSON value target.
 */
public final class JSONValueTarget extends JSONTarget<JSONValue> {

    private final ArrayDeque<JSONValue> stack;

    private final ArrayDeque<String> names;

    private boolean needSeparator;

    private String memberName;

    private JSONValue result;

    /**
     * Creates new instance of JSON value target.
     */
    public JSONValueTarget() {
        stack = new ArrayDeque<>();
        names = new ArrayDeque<>();
    }

    @Override
    public void startObject() {
        beforeValue();
        names.push(memberName != null ? memberName : "");
        memberName = null;
        stack.push(new JSONObject());
    }

    @Override
    public void endObject() {
        if (memberName != null) {
            throw new IllegalStateException();
        }
        JSONValue value = stack.poll();
        if (!(value instanceof JSONObject)) {
            throw new IllegalStateException();
        }
        memberName = names.pop();
        afterValue(value);
    }

    @Override
    public void startArray() {
        beforeValue();
        names.push(memberName != null ? memberName : "");
        memberName = null;
        stack.push(new JSONArray());
    }

    @Override
    public void endArray() {
        JSONValue value = stack.poll();
        if (!(value instanceof JSONArray)) {
            throw new IllegalStateException();
        }
        memberName = names.pop();
        afterValue(value);
    }

    @Override
    public void member(String name) {
        if (memberName != null || !(stack.peek() instanceof JSONObject)) {
            throw new IllegalStateException();
        }
        memberName = name;
        beforeValue();
    }

    @Override
    public void valueNull() {
        beforeValue();
        afterValue(JSONNull.NULL);
    }

    @Override
    public void valueFalse() {
        beforeValue();
        afterValue(JSONBoolean.FALSE);
    }

    @Override
    public void valueTrue() {
        beforeValue();
        afterValue(JSONBoolean.TRUE);
    }

    @Override
    public void valueNumber(BigDecimal number) {
        beforeValue();
        afterValue(new JSONNumber(number));
    }

    @Override
    public void valueString(String string) {
        beforeValue();
        afterValue(new JSONString(string));
    }

    private void beforeValue() {
        if (memberName == null && stack.peek() instanceof JSONObject) {
            throw new IllegalStateException();
        }
        if (needSeparator) {
            if (stack.isEmpty()) {
                throw new IllegalStateException();
            }
            needSeparator = false;
        }
    }

    private void afterValue(JSONValue value) {
        JSONValue parent = stack.peek();
        if (parent == null) {
            result = value;
        } else if (parent instanceof JSONObject) {
            ((JSONObject) parent).addMember(memberName, value);
        } else {
            ((JSONArray) parent).addElement(value);
        }
        needSeparator = true;
        memberName = null;
    }

    @Override
    public boolean isPropertyExpected() {
        return memberName == null && stack.peek() instanceof JSONObject;
    }

    @Override
    public boolean isValueSeparatorExpected() {
        return needSeparator;
    }

    @Override
    public JSONValue getResult() {
        if (!stack.isEmpty() || result == null) {
            throw new IllegalStateException();
        }
        return result;
    }

}
