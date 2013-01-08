/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;

/**
 * A string type.
 */
public class StringDataType implements DataType {

    public static final StringDataType INSTANCE = new StringDataType();

    public int compare(Object a, Object b) {
        return a.toString().compareTo(b.toString());
    }

    public int getMemory(Object obj) {
        return 24 + 2 * obj.toString().length();
    }

    public String read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        return DataUtils.readString(buff, len);
    }

    public ByteBuffer write(ByteBuffer buff, Object obj) {
        String s = obj.toString();
        int len = s.length();
        DataUtils.writeVarInt(buff, len);
        return DataUtils.writeStringData(buff, s, len);
    }

}

