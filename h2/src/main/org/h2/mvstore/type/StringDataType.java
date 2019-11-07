/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

/**
 * A string type.
 */
public class StringDataType extends BasicDataType<String> {

    public static final StringDataType INSTANCE = new StringDataType();

    @Override
    public String[] createStorage(int size) {
        return new String[size];
    }

    @Override
    public int compare(String a, String b) {
        return a.compareTo(b);
    }

    @Override
    public int getMemory(String obj) {
        return 24 + 2 * obj.length();
    }

    @Override
    public String read(ByteBuffer buff) {
        return DataUtils.readString(buff);
    }

    @Override
    public void write(WriteBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }
}

