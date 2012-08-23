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
public class StringType implements DataType {

    public int compare(Object a, Object b) {
        return a.toString().compareTo(b.toString());
    }

    public int getMaxLength(Object obj) {
        return DataUtils.MAX_VAR_INT_LEN + obj.toString().length() * 3;
    }

    public int getMemory(Object obj) {
        return obj.toString().length() * 2 + 48;
    }

    public String read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff.get() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) + ((buff.get() & 0x3f) << 6) + (buff.get() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff.get() & 0x3f));
            }
        }
        return new String(chars);
    }

    public void write(ByteBuffer buff, Object obj) {
        String s = obj.toString();
        int len = s.length();
        DataUtils.writeVarInt(buff, len);
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff.put((byte) c);
            } else if (c >= 0x800) {
                buff.put((byte) (0xe0 | (c >> 12)));
                buff.put((byte) (((c >> 6) & 0x3f)));
                buff.put((byte) (c & 0x3f));
            } else {
                buff.put((byte) (0xc0 | (c >> 6)));
                buff.put((byte) (c & 0x3f));
            }
        }
    }

    public String asString() {
        return "";
    }

}

