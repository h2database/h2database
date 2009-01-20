/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.util.StringUtils;

/**
 * The base class for all ValueString* classes.
 */
abstract class ValueStringBase extends Value {

    /**
     * The string data.
     */
    protected final String value;

    protected ValueStringBase(String value) {
        this.value = value;
    }

    public String getSQL() {
        return StringUtils.quoteStringSQL(value);
    }

    public String getString() {
        return value;
    }

    public long getPrecision() {
        return value.length();
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setString(parameterIndex, value);
    }

    public int getDisplaySize() {
        return value.length();
    }

    public abstract Value convertPrecision(long precision);

    public int getMemory() {
        return value.length() * 2 + 30;
    }

}
