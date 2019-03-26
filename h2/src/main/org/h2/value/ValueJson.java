/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Lazarev Nikita <lazarevn@ispras.ru>
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.util.StringUtils;

/**
 * Implementation of the JSON data type.
 */
public class ValueJson extends Value {

    private String value;

    ValueJson (String s) {
        this.value = s;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        return StringUtils.quoteStringSQL(builder, value).append("::JSON");
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_JSON;
    }

    @Override
    public int getValueType() {
        return Value.JSON;
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public int getMemory() {
        return value.length() * 2 + 94;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setString(parameterIndex, value);
    }

    /*
     * The simplest version
     * In fact {"foo":1,"bar":2} must be equal to {"bar":2, "foo":1}
     */
    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /*
     * Similar to hashCode()
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof ValueJson &&
                this.value.equals(((ValueJson) other).value);
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        String other = ((ValueJson) v).value;
        return mode.compareString(value, other, false);
    }

    public static Value get(String s) {
        return new ValueJson(s);
    }

}
