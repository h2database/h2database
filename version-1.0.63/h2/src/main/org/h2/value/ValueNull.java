/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
 * @author Thomas
 */
public class ValueNull extends Value {

    public static final ValueNull INSTANCE = new ValueNull();
    public static final ValueNull DELETED = new ValueNull();

    public static final int PRECISION = 1;
    public static final int DISPLAY_SIZE = 4; // null

    private ValueNull() {
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

    public Boolean getBoolean() throws SQLException {
        return null;
    }

    public Date getDate() throws SQLException {
        return null;
    }

    public Time getTime() throws SQLException {
        return null;
    }

    public Timestamp getTimestamp() throws SQLException {
        return null;
    }

    public byte[] getBytes() throws SQLException {
        return null;
    }

    public byte getByte() throws SQLException {
        return 0;
    }

    public short getShort() throws SQLException {
        return 0;
    }

    public BigDecimal getBigDecimal() throws SQLException {
        return null;
    }

    public double getDouble() throws SQLException {
        return 0.0;
    }

    public float getFloat() throws SQLException {
        return 0.0F;
    }

    public int getInt() throws SQLException {
        return 0;
    }

    public long getLong() throws SQLException {
        return 0;
    }

    public InputStream getInputStream() throws SQLException {
        return null;
    }

    public Reader getReader() throws SQLException {
        return null;
    }

    public Value convertTo(int type) throws SQLException {
        return this;
    }

    protected int compareSecure(Value v, CompareMode mode) {
        throw Message.getInternalError("compare null");
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

    protected boolean isEqual(Value v) {
        return v == ValueNull.INSTANCE;
    }

}
