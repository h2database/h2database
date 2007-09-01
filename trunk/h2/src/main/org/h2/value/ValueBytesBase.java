/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.util.ByteUtils;

abstract class ValueBytesBase extends Value {

    private final byte[] value;
    private int hash;
    
    protected ValueBytesBase(byte[] v) {
        this.value = v;
    }

    public String getSQL() {
        return "X'" + getString() + "'";
    }

    public byte[] getBytesNoCopy() {
        return value;
    }

    public byte[] getBytes() {
        return ByteUtils.cloneByteArray(value);
    }

    protected int compareSecure(Value v, CompareMode mode) {
        byte[] v2 = ((ValueBytesBase) v).value;
        return ByteUtils.compareNotNull(value, v2);
    }

    public String getString() {
        return ByteUtils.convertBytesToString(value);
    }

    public long getPrecision() {
        return value.length;
    }

    public int hashCode() {
        if (hash == 0) {
            hash = ByteUtils.getByteArrayHash(value);
        }
        return hash;
    }

    public Object getObject() {
        return getBytes();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBytes(parameterIndex, value);
    }

    public int getDisplaySize() {
        return value.length * 2;
    }    
    
    protected boolean isEqual(Value v) {
        return v instanceof ValueBytesBase && ByteUtils.compareNotNull(value, ((ValueBytesBase) v).value) == 0;
    }    

//  public String getJavaString() {
//  return "ByteUtils.convertStringToBytes(\"" + toString() + "\")";
//}

}
