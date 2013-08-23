/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.util.List;
import org.h2.jdbc.JdbcSQLException;

/**
 * A JDBC SQL Exception with additional parameters, it provides the syntax error
 * position and expected token.
 * <p>
 * When the H2 parser encounters an error, it normally throws one of these
 * exceptions. Clients may use this information to implement things like
 * autocomplete in editors, or just better display of errors.
 * 
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class JdbcParseSQLException extends JdbcSQLException {
    
    private final List<String> expectedTokens;
    private final int syntaxErrorPosition;

    /**
     * Creates a JdbcParseSQLException.
     * 
     * @param message the reason
     * @param sql the SQL statement
     * @param state the SQL state
     * @param errorCode the error code
     * @param cause the exception that was the reason for this exception
     * @param stackTrace the stack trace
     * @param expectedTokens H2 parser expected tokens
     * @param syntaxErrorPosition Syntax error character index
     */
    public JdbcParseSQLException(String message, String sql, String state, int errorCode, Throwable cause,
            String stackTrace, List<String> expectedTokens, int syntaxErrorPosition) {
        super(message, sql, state, errorCode, cause, stackTrace);
        this.expectedTokens = expectedTokens;
        this.syntaxErrorPosition = syntaxErrorPosition;
    }

    /**
     * H2 parser expected tokens
     */
    public List<String> getExpectedTokens() {
        return expectedTokens;
    }

    /**
     * Syntax error character position
     */
    public int getSyntaxErrorPosition() {
        return syntaxErrorPosition;
    }
}
