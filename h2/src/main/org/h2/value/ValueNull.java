/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.message.Message;

/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public class ValueNull extends Value {

    /**
     * The main NULL instance.
     */
    public static final ValueNull INSTANCE = new ValueNull();

    /**
     * This special instance is used as a marker for deleted entries in a map.
     * It should not be used anywhere else.
     */
    public static final ValueNull DELETED = new ValueNull();

    /**
     * The precision of NULL.
     */
    private static final int PRECISION = 1;

    /**
     * The display size of the textual representation of NULL.
     */
    private static final int DISPLAY_SIZE = 4;

    private ValueNull() {
        // don't allow construction
    }

    public String getSQL() {
        return "NULL";
    }

    public int getType() {
        return Value.NULL;
    }

    public String getString() {
        return null;
    }

    public Boolean getBoolean() {
        return null;
    }

    public Date getDate() {
        return null;
    }

    public Time getTime() {
        return null;
    }

    public Timestamp getTimestamp() {
        return null;
    }

    public byte[] getBytes() {
        return null;
    }

    public byte getByte() {
        return 0;
    }

    public short getShort() {
        return 0;
    }

    public BigDecimal getBigDecimal() {
        return null;
    }

    public double getDouble() {
        return 0.0;
    }

    public float getFloat() {
        return 0.0F;
    }

    public int getInt() {
        return 0;
    }

    public long getLong() {
        return 0;
    }

    public InputStream getInputStream() {
        return null;
    }

    public Reader getReader() {
        return null;
    }

    public Value convertTo(int type) {
        return this;
    }

    protected int compareSecure(Value v, CompareMode mode) {
        throw Message.throwInternalError("compare null");
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return 0;
    }

    public Object getObject() {
        return null;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setNull(parameterIndex, DataType.convertTypeToSQLType(Value.NULL));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other == this;
    }

}
