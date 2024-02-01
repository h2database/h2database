/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;

import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;

/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public final class ValueNull extends Value {

    /**
     * The main NULL instance.
     */
    public static final ValueNull INSTANCE = new ValueNull();

    /**
     * The precision of NULL.
     */
    static final int PRECISION = 1;

    /**
     * The display size of the textual representation of NULL.
     */
    static final int DISPLAY_SIZE = 4;

    private ValueNull() {
        // don't allow construction
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append("NULL");
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_NULL;
    }

    @Override
    public int getValueType() {
        return NULL;
    }

    @Override
    public int getMemory() {
        // Singleton value
        return 0;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public Reader getReader() {
        return null;
    }

    @Override
    public Reader getReader(long oneBasedOffset, long length) {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public InputStream getInputStream() {
        return null;
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        return null;
    }

    @Override
    public boolean getBoolean() {
        throw DbException.getInternalError();
    }

    @Override
    public byte getByte() {
        throw DbException.getInternalError();
    }

    @Override
    public short getShort() {
        throw DbException.getInternalError();
    }

    @Override
    public int getInt() {
        throw DbException.getInternalError();
    }

    @Override
    public long getLong() {
        throw DbException.getInternalError();
    }

    @Override
    public BigDecimal getBigDecimal() {
        return null;
    }

    @Override
    public float getFloat() {
        throw DbException.getInternalError();
    }

    @Override
    public double getDouble() {
        throw DbException.getInternalError();
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        throw DbException.getInternalError("compare null");
    }

    @Override
    public boolean containsNull() {
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

}
