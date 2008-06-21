/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;

/*## Java 1.6 begin ##   
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
## Java 1.6 end ##*/

/**
 * Messages used in the database engine.
 * Use the PropertiesToUTF8 tool to translate properties files to UTF-8 and back.
 * If the word 'SQL' appears then the whole SQL statement must be a parameter,
 * otherwise this may be added: '; SQL statement: ' + sql
 */
public class Message {
    
    private Message() {
        // utility class
    }
    
    /**
     * Convert an exception to a SQL exception using the default mapping.
     * 
     * @param e the root cause
     * @return the SQL exception object
     */
/*## Java 1.6 begin ##    
    public static SQLException convert(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        } else if (e instanceof InvocationTargetException) {
            InvocationTargetException te = (InvocationTargetException) e;
            Throwable t = te.getTargetException();
            if (t instanceof SQLException) {
                return (SQLException) t;
            }
            return new SQLException("Invocation exception: " + t.toString(), e);
        } else if (e instanceof IOException) {
            return new SQLException("IO exception: " + e.toString(), e);
        }
        return new SQLException("General exception: " + e.toString(), e);
    }
## Java 1.6 end ##*/

}
