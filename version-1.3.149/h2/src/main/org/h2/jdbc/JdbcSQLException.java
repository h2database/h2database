/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

    /**
     * If the SQL statement contains this text, then it is never added to the
     * SQL exception. Hiding the SQL statement may be important if it contains a
     * passwords, such as a CREATE LINKED TABLE statement.
     */
    public static final String HIDE_SQL = "--hide--";

    private static final long serialVersionUID = 1L;
    private final String originalMessage;
    private final Throwable cause;
    private final String stackTrace;
    private String message;
    private String sql;

    /**
     * Creates a SQLException.
     *
     * @param message the reason
     * @param sql the SQL statement
     * @param state the SQL state
     * @param errorCode the error code
     * @param cause the exception that was the reason for this exception
     * @param stackTrace the stack trace
     */
    public JdbcSQLException(String message, String sql, String state, int errorCode, Throwable cause, String stackTrace) {
        super(message, state, errorCode);
        this.originalMessage = message;
        setSQL(sql);
        this.cause = cause;
        this.stackTrace = stackTrace;
        buildMessage();
//## Java 1.4 begin ##
        initCause(cause);
//## Java 1.4 end ##
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
            /*## Java 1.3 only begin ##
            if (cause != null) {
                cause.printStackTrace(s);
            }
            ## Java 1.3 only end ##*/
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
            /*## Java 1.3 only begin ##
            if (cause != null) {
                cause.printStackTrace(s);
            }
            ## Java 1.3 only end ##*/
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
     * SQL statements that contain '--hide--' are not listed.
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
        if (sql != null && sql.indexOf(HIDE_SQL) >= 0) {
            sql = "-";
        }
        this.sql = sql;
        buildMessage();
    }

    private void buildMessage() {
        StringBuilder buff = new StringBuilder(originalMessage == null ? "- " : originalMessage);
        if (sql != null) {
            buff.append("; SQL statement:\n").append(sql);
        }
        buff.append(" [").append(getErrorCode()).append('-').append(Constants.BUILD_ID).append(']');
        message = buff.toString();
    }

    /**
     * Returns the class name, the message, and in the server mode, the stack
     * trace of the server
     *
     * @return the string representation
     */
    public String toString() {
        if (stackTrace == null) {
            return super.toString();
        }
        return stackTrace;
    }

}
