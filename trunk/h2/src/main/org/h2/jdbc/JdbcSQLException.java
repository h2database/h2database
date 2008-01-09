/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.h2.engine.Constants;

/**
 * Represents a database exception.
 */
public class JdbcSQLException extends SQLException {

    private static final long serialVersionUID = -8200821788226954151L;
    private final String originalMessage;
    private final Throwable cause;
    private final String trace;
    private String message;
    private String sql;

    /**
     * Creates a SQLException a message, sqlstate and cause.
     *
     * @param message the reason
     * @param state the SQL state
     * @param cause the exception that was the reason for this exception
     */
    public JdbcSQLException(String message, String sql, String state, int errorCode, Throwable cause, String trace) {
        super(message, state, errorCode);
        this.originalMessage = message;
        this.sql = sql;
        this.cause = cause;
        this.trace = trace;
        buildMessage();
//#ifdef JDK14
        initCause(cause);
//#endif
    }

    /**
     * Get the detail error message.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
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
        // The default implementation already does that,
        // but we do it again to avoid problems.
        // If it is not implemented, somebody might implement it
        // later on which would be a problem if done in the wrong way.
        printStackTrace(System.err);
    }

    /**
     * Prints the stack trace to the specified print writer.
     *
     * @param s the print writer
     */
    public void printStackTrace(PrintWriter s) {
        if (s != null) {
            super.printStackTrace(s);
//#ifdef JDK13
/*
            if (cause != null) {
                cause.printStackTrace(s);
            }
*/
//#endif
            // getNextException().printStackTrace(s) would be very very slow
            // if many exceptions are joined
            SQLException next = getNextException();
            for (int i = 0; i < 100 && next != null; i++) {
                s.println(next.toString());
                next = next.getNextException();
            }
            if (next != null) {
                s.println("(truncated)");
            }
        }
    }

    /**
     * Prints the stack trace to the specified print stream.
     *
     * @param s the print stream
     */
    public void printStackTrace(PrintStream s) {
        if (s != null) {
            super.printStackTrace(s);
//#ifdef JDK13
/*
            if (cause != null) {
                cause.printStackTrace(s);
            }
*/
//#endif
            // getNextException().printStackTrace(s) would be very very slow
            // if many exceptions are joined
            SQLException next = getNextException();
            for (int i = 0; i < 100 && next != null; i++) {
                s.println(next.toString());
                next = next.getNextException();
            }
            if (next != null) {
                s.println("(truncated)");
            }
        }
    }

    /**
     * INTERNAL
     */
    public Throwable getOriginalCause() {
        return cause;
    }

    /**
     * Returns the SQL statement.
     *
     * @return the SQL statement
     */
    public String getSQL() {
        return sql;
    }

    /**
     * INTERNAL
     */
    public void setSQL(String sql) {
        this.sql = sql;
        buildMessage();
    }

    private void buildMessage() {
        StringBuffer buff = new StringBuffer(originalMessage == null ? "- " : originalMessage);
        if (sql != null) {
            buff.append("; SQL statement:\n");
            buff.append(sql);
        }
        buff.append(" [");
        buff.append(getSQLState());
        buff.append('-');
        buff.append(Constants.BUILD_ID);
        buff.append(']');
        message = buff.toString();
    }

    /**
     * Returns the class name, the message, and in the server mode, the stack trace of the server
     *
     * @return the string representation
     */
    public String toString() {
        if (trace == null) {
            return super.toString();
        } else {
            return trace;
        }
    }

}
