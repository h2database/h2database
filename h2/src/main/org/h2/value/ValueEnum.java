/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.engine.SysProperties;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the ENUM data type.
 */
public class ValueEnum extends ValueString {
    protected ValueEnum(String value) {
        super(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueEnum
                && value.equals(((ValueEnum) other).value);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        // compatibility: the other object could be another type
        ValueEnum v = (ValueEnum) o;
        return mode.compareString(value, v.value, false);
    }

    @Override
    public int getType() {
        return Value.ENUM;
    }

    /**
     * Create a new String value of the current class.
     * This method is meant to be overridden by subclasses.
     *
     * @param s the string
     * @return the value
     */
    protected Value getNew(String s) {
        return ValueEnum.get(s);
    }

}
