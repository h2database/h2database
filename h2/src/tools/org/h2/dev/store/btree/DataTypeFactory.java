/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

import java.io.IOException;
import java.io.StringReader;

/**
 * A factory for data types.
 */
public class DataTypeFactory {

    /**
     * Read the data type.
     *
     * @param buff the buffer
     * @return the type
     */
    static DataType fromString(String s) {
        StringReader r = new StringReader(s);
        char c;
        try {
            c = (char) r.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        switch (c) {
        case 'i':
            return new IntegerType();
        }
        throw new RuntimeException("Unknown data type " + c);
    }

    /**
     * Get the data type object for the given class.
     *
     * @param objectClass the class
     * @return the data type object
     */
    static DataType getDataType(Class<?> objectClass) {
        if (objectClass == Integer.class) {
            return new IntegerType();
        } else if (objectClass == String.class) {
            return new StringType();
        }
        throw new RuntimeException("Unsupported object class " + objectClass.toString());
    }

}
