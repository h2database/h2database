/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.jdbc.JdbcBlob;
import org.h2.jdbc.JdbcClob;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;


/**
 * @author Thomas
 */

public class TypeConverter {

    public static Object getDefaultForPrimitiveType(Class clazz) {
        if(clazz == Boolean.TYPE) {
            return Boolean.FALSE;
        } else if(clazz == Byte.TYPE) {
            return new Byte((byte)0);
        } else if(clazz == Character.TYPE) {
            return new Character((char)0);
        } else if(clazz == Short.TYPE) {
            return new Short((short)0);
        } else if(clazz == Integer.TYPE) {
            return new Integer(0);
        } else if(clazz == Long.TYPE) {
            return new Long(0);
        } else if(clazz == Float.TYPE) {
            return new Float(0);
        } else if(clazz == Double.TYPE) {
            return new Double(0);
        } else {
            throw Message.getInternalError("primitive="+ clazz.toString());
        }
    }

    public static byte[] serialize(Object obj) throws SQLException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch(Throwable e) {
            throw Message.getSQLException(Message.SERIALIZATION_FAILED, null, e);
        }
    }

    public static Object deserialize(byte[] data) throws SQLException {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            Object obj = is.readObject();
            return obj;
        } catch(Throwable e) {
            throw Message.getSQLException(Message.DESERIALIZATION_FAILED, null, e);
        }
    }

    public static Reader getReader(InputStream in) throws SQLException {
        try {
            // InputStreamReader may read some more bytes
            return in == null ? null : new InputStreamReader(in, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw Message.convert(e);
        }
    }

    public static InputStream getInputStream(String s) throws SQLException {
        return new ByteArrayInputStream(StringUtils.utf8Encode(s));
    }

    public static InputStream getInputStream(Reader x) throws SQLException {
        return x == null ? null : new ReaderInputStream(x);
    }

    public static Reader getReader(String s) {
        return new StringReader(s);
    }

    public static Date convertDateToCalendar(Date x, Calendar calendar) throws SQLException {
        return x == null ? x : new Date(getLocalTime(x, calendar));
    }

    public static Time convertTimeToCalendar(Time x, Calendar calendar) throws SQLException {
        return x == null ? x : new Time(getLocalTime(x, calendar));
    }

    public static Timestamp convertTimestampToCalendar(Timestamp x, Calendar calendar) throws SQLException {
        if(x != null) {
            Timestamp y = new Timestamp(getLocalTime(x, calendar));
            // fix the nano seconds
            y.setNanos(x.getNanos());
            x = y;
        }
        return x;
    }

    public static Value convertDateToUniversal(Date x, Calendar source) throws SQLException {
        return ValueDate.get(new Date(TypeConverter.getUniversalTime(source, x)));
    }

    public static Value convertTimeToUniversal(Time x, Calendar source) throws SQLException {
        return ValueTime.get(new Time(TypeConverter.getUniversalTime(source, x)));
    }

    public static Value convertTimestampToUniversal(Timestamp x, Calendar source) throws SQLException {
        Timestamp y = new Timestamp(TypeConverter.getUniversalTime(source, x));
        // fix the nano seconds
        y.setNanos(x.getNanos());
        return ValueTimestamp.get(y);
    }

    private static long getUniversalTime(Calendar source, java.util.Date x) throws SQLException {
        if(source == null) {
            throw Message.getInvalidValueException("calendar", null);
        }
        source = (Calendar)source.clone();
        Calendar universal=Calendar.getInstance();
        source.setTime(x);
        convertTime(source, universal);
        return universal.getTime().getTime();
    }

    private static long getLocalTime(java.util.Date x, Calendar target) throws SQLException {
        if(target == null) {
            throw Message.getInvalidValueException("calendar", null);
        }
        target = (Calendar)target.clone();
        Calendar local=Calendar.getInstance();
        local.setTime(x);
        convertTime(local, target);
        return target.getTime().getTime();
    }

    private static void convertTime(Calendar from, Calendar to) {
        to.set(Calendar.YEAR, from.get(Calendar.YEAR));
        to.set(Calendar.MONTH, from.get(Calendar.MONTH));
        to.set(Calendar.DAY_OF_MONTH, from.get(Calendar.DAY_OF_MONTH));
        to.set(Calendar.HOUR_OF_DAY, from.get(Calendar.HOUR_OF_DAY));
        to.set(Calendar.MINUTE, from.get(Calendar.MINUTE));
        to.set(Calendar.SECOND, from.get(Calendar.SECOND));
        to.set(Calendar.MILLISECOND, from.get(Calendar.MILLISECOND));
    }

    public static Reader getAsciiReader(InputStream x) throws SQLException {
        try {
            return x == null ? null : new InputStreamReader(x, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw Message.convert(e);
        }
    }

    public static Object convertTo(SessionInterface session, JdbcConnection conn, Value v, Class paramClass) throws JdbcSQLException {
        if(paramClass == java.sql.Blob.class) {
            return new JdbcBlob(session, conn, v, 0);
        } else if(paramClass == Clob.class) {
            return new JdbcClob(session, conn, v, 0);
        } else {
            throw Message.getUnsupportedException();
        }
    }

}
