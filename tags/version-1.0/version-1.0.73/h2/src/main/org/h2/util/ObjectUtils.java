/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;

/**
 * Utility class for object creation and serialization.
 */
public class ObjectUtils {
    
    private ObjectUtils() {
        // utility class
    }

    public static Integer getInteger(int x) {
/*## Java 1.5 begin ##
        if (true) {
            return Integer.valueOf(x);
        }
## Java 1.5 end ##*/
        // NOPMD
        return new Integer(x); 
    }

    public static Character getCharacter(char x) {
/*## Java 1.5 begin ##
        if (true) {
            return Character.valueOf(x);
        }
## Java 1.5 end ##*/
        return new Character(x);
    }

    public static Long getLong(long x) {
/*## Java 1.5 begin ##
        if (true) {
            return Long.valueOf(x);
        }
## Java 1.5 end ##*/
        // NOPMD
        return new Long(x); 
    }

    public static Short getShort(short x) {
/*## Java 1.5 begin ##
        if (true) {
            return Short.valueOf(x);
        }
## Java 1.5 end ##*/
        // NOPMD
        return new Short(x); 
    }

    public static Byte getByte(byte x) {
/*## Java 1.5 begin ##
        if (true) {
            return Byte.valueOf(x);
        }
## Java 1.5 end ##*/
        // NOPMD
        return new Byte(x); 
    }

    public static Float getFloat(float x) {
/*## Java 1.5 begin ##
        if (true) {
            return Float.valueOf(x);
        }
## Java 1.5 end ##*/
        return new Float(x);
    }

    public static Double getDouble(double x) {
/*## Java 1.5 begin ##
        if (true) {
            return Double.valueOf(x);
        }
## Java 1.5 end ##*/
        return new Double(x);
    }

    public static byte[] serialize(Object obj) throws SQLException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.SERIALIZATION_FAILED_1, new String[] { e.toString() }, e);
        }
    }

    public static Object deserialize(byte[] data) throws SQLException {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            Object obj = is.readObject();
            return obj;
        } catch (Throwable e) {
            throw Message.getSQLException(ErrorCode.DESERIALIZATION_FAILED_1, new String[] { e.toString() }, e);
        }
    }

}
