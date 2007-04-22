/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.h2.engine.Constants;

/**
 * Represents an exception.
 */
public class JdbcSQLException extends SQLException {

    private static final long serialVersionUID = -8200821788226954151L;
    private Throwable cause;
    private String originalMessage;

    /**
     * Creates a SQLException a message, sqlstate and cause.
     *
     * @param message the reason
     * @param state the SQL state
     * @param cause the exception that was the reason for this exception
     */
    public JdbcSQLException(String message, String state, int errorCode, Throwable cause) {
        super(message + " [" + state + "-" + Constants.BUILD_ID + "]", state, errorCode);
        this.originalMessage = message;
        this.cause = cause;
//#ifdef JDK14
        initCause(cause);
//#endif        
    }

    /**
     * INTERNAL
     */
    public String getOriginalMessage() {
        return originalMessage;
    }

    /**
     * Prints the stack trace to the standard error stream.
     */
    public void printStackTrace() {
        super.printStackTrace();
//#ifdef JDK13    
/*
        if (cause != null) {
            cause.printStackTrace();
        }
*/
//#endif    
        if(getNextException() != null) {
            getNextException().printStackTrace();
        }
    }

    /**
     * Prints the stack trace to the specified print writer.
     *
     * @param s the print writer
     */
    public void printStackTrace(PrintWriter s) {
        if(s!=null) {
            super.printStackTrace(s);
//#ifdef JDK13
/*
            if (cause != null) {
                cause.printStackTrace(s);
            }
*/
//#endif    
            if(getNextException() != null) {
                getNextException().printStackTrace(s);
            }
        }
    }

    /**
     * Prints the stack trace to the specified print stream.
     *
     * @param s the print stream
     */
    public void printStackTrace(PrintStream s) {
        if(s!=null) {
            super.printStackTrace(s);
//#ifdef JDK13
/*
            if (cause != null) {
                cause.printStackTrace(s);
            }
*/
//#endif    
            if(getNextException() != null) {
                getNextException().printStackTrace(s);
            }
        }
    }

    /**
     * INTERNAL
     */
    public Throwable getOriginalCause() {
        return cause;
    }

}
