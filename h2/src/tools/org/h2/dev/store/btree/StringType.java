/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.nio.ByteBuffer;

/**
 * A string type.
 */
class StringType implements DataType {

    public int compare(Object a, Object b) {
        return a.toString().compareTo(b.toString());
    }

    public int length(Object obj) {
        try {
            byte[] bytes = obj.toString().getBytes("UTF-8");
            return DataUtils.getVarIntLen(bytes.length) + bytes.length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        byte[] bytes = new byte[len];
        buff.get(bytes);
        try {
            return new String(bytes, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void write(ByteBuffer buff, Object x) {
        try {
            byte[] bytes = x.toString().getBytes("UTF-8");
            DataUtils.writeVarInt(buff, bytes.length);
            buff.put(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String asString() {
        return "";
    }

}

