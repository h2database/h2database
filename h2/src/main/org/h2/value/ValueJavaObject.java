/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.Constants;

public class ValueJavaObject extends ValueBytesBase {
    
    private static final ValueJavaObject EMPTY = new ValueJavaObject(new byte[0]);

    protected ValueJavaObject(byte[] v) {
        super(v);
    }
    
    public static ValueJavaObject get(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        ValueJavaObject obj = new ValueJavaObject(b);
        if (b.length > Constants.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueJavaObject) Value.cache(obj);
    }
    
    public int getType() {
        return Value.JAVA_OBJECT;
    }

}
