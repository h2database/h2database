/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import static org.h2.util.ParserUtil.IDENTIFIER;
import static org.h2.util.ParserUtil.LAST_KEYWORD;

import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueInteger;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * Token.
 */
public abstract class Token implements Cloneable {

    /**
     * Token with parameter.
     */
    static final int PARAMETER = LAST_KEYWORD + 1;

    /**
     * End of input.
     */
    static final int END_OF_INPUT = PARAMETER + 1;

    /**
     * Token with literal.
     */
    static final int LITERAL = END_OF_INPUT + 1;

    /**
     * The token "=".
     */
    static final int EQUAL = LITERAL + 1;

    /**
     * The token "&gt;=".
     */
    static final int BIGGER_EQUAL = EQUAL + 1;

    /**
     * The token "&gt;".
     */
    static final int BIGGER = BIGGER_EQUAL + 1;

    /**
     * The token "&lt;".
     */
    static final int SMALLER = BIGGER + 1;

    /**
     * The token "&lt;=".
     */
    static final int SMALLER_EQUAL = SMALLER + 1;

    /**
     * The token "&lt;&gt;" or "!=".
     */
    static final int NOT_EQUAL = SMALLER_EQUAL + 1;

    /**
     * The token "@".
     */
    static final int AT = NOT_EQUAL + 1;

    /**
     * The token "-".
     */
    static final int MINUS_SIGN = AT + 1;

    /**
     * The token "+".
     */
    static final int PLUS_SIGN = MINUS_SIGN + 1;

    /**
     * The token "||".
     */
    static final int CONCATENATION = PLUS_SIGN + 1;

    /**
     * The token "(".
     */
    static final int OPEN_PAREN = CONCATENATION + 1;

    /**
     * The token ")".
     */
    static final int CLOSE_PAREN = OPEN_PAREN + 1;

    /**
     * The token "&amp;&amp;".
     */
    static final int SPATIAL_INTERSECTS = CLOSE_PAREN + 1;

    /**
     * The token "*".
     */
    static final int ASTERISK = SPATIAL_INTERSECTS + 1;

    /**
     * The token ",".
     */
    static final int COMMA = ASTERISK + 1;

    /**
     * The token ".".
     */
    static final int DOT = COMMA + 1;

    /**
     * The token "{".
     */
    static final int OPEN_BRACE = DOT + 1;

    /**
     * The token "}".
     */
    static final int CLOSE_BRACE = OPEN_BRACE + 1;

    /**
     * The token "/".
     */
    static final int SLASH = CLOSE_BRACE + 1;

    /**
     * The token "%".
     */
    static final int PERCENT = SLASH + 1;

    /**
     * The token ";".
     */
    static final int SEMICOLON = PERCENT + 1;

    /**
     * The token ":".
     */
    static final int COLON = SEMICOLON + 1;

    /**
     * The token "[".
     */
    static final int OPEN_BRACKET = COLON + 1;

    /**
     * The token "]".
     */
    static final int CLOSE_BRACKET = OPEN_BRACKET + 1;

    /**
     * The token "~".
     */
    static final int TILDE = CLOSE_BRACKET + 1;

    /**
     * The token "::".
     */
    static final int COLON_COLON = TILDE + 1;

    /**
     * The token ":=".
     */
    static final int COLON_EQ = COLON_COLON + 1;

    /**
     * The token "!~".
     */
    static final int NOT_TILDE = COLON_EQ + 1;

    static final String[] TOKENS = {
            // Unused
            null,
            // KEYWORD
            null,
            // IDENTIFIER
            null,
            // ALL
            "ALL",
            // AND
            "AND",
            // ANY
            "ANY",
            // ARRAY
            "ARRAY",
            // AS
            "AS",
            // ASYMMETRIC
            "ASYMMETRIC",
            // AUTHORIZATION
            "AUTHORIZATION",
            // BETWEEN
            "BETWEEN",
            // CASE
            "CASE",
            // CAST
            "CAST",
            // CHECK
            "CHECK",
            // CONSTRAINT
            "CONSTRAINT",
            // CROSS
            "CROSS",
            // CURRENT_CATALOG
            "CURRENT_CATALOG",
            // CURRENT_DATE
            "CURRENT_DATE",
            // CURRENT_PATH
            "CURRENT_PATH",
            // CURRENT_ROLE
            "CURRENT_ROLE",
            // CURRENT_SCHEMA
            "CURRENT_SCHEMA",
            // CURRENT_TIME
            "CURRENT_TIME",
            // CURRENT_TIMESTAMP
            "CURRENT_TIMESTAMP",
            // CURRENT_USER
            "CURRENT_USER",
            // DAY
            "DAY",
            // DEFAULT
            "DEFAULT",
            // DISTINCT
            "DISTINCT",
            // ELSE
            "ELSE",
            // END
            "END",
            // EXCEPT
            "EXCEPT",
            // EXISTS
            "EXISTS",
            // FALSE
            "FALSE",
            // FETCH
            "FETCH",
            // FOR
            "FOR",
            // FOREIGN
            "FOREIGN",
            // FROM
            "FROM",
            // FULL
            "FULL",
            // GROUP
            "GROUP",
            // HAVING
            "HAVING",
            // HOUR
            "HOUR",
            // IF
            "IF",
            // IN
            "IN",
            // INNER
            "INNER",
            // INTERSECT
            "INTERSECT",
            // INTERVAL
            "INTERVAL",
            // IS
            "IS",
            // JOIN
            "JOIN",
            // KEY
            "KEY",
            // LEFT
            "LEFT",
            // LIKE
            "LIKE",
            // LIMIT
            "LIMIT",
            // LOCALTIME
            "LOCALTIME",
            // LOCALTIMESTAMP
            "LOCALTIMESTAMP",
            // MINUS
            "MINUS",
            // MINUTE
            "MINUTE",
            // MONTH
            "MONTH",
            // NATURAL
            "NATURAL",
            // NOT
            "NOT",
            // NULL
            "NULL",
            // OFFSET
            "OFFSET",
            // ON
            "ON",
            // OR
            "OR",
            // ORDER
            "ORDER",
            // PRIMARY
            "PRIMARY",
            // QUALIFY
            "QUALIFY",
            // RIGHT
            "RIGHT",
            // ROW
            "ROW",
            // ROWNUM
            "ROWNUM",
            // SECOND
            "SECOND",
            // SELECT
            "SELECT",
            // SESSION_USER
            "SESSION_USER",
            // SET
            "SET",
            // SOME
            "SOME",
            // SYMMETRIC
            "SYMMETRIC",
            // SYSTEM_USER
            "SYSTEM_USER",
            // TABLE
            "TABLE",
            // TO
            "TO",
            // TRUE
            "TRUE",
            // UESCAPE
            "UESCAPE",
            // UNION
            "UNION",
            // UNIQUE
            "UNIQUE",
            // UNKNOWN
            "UNKNOWN",
            // USER
            "USER",
            // USING
            "USING",
            // VALUE
            "VALUE",
            // VALUES
            "VALUES",
            // WHEN
            "WHEN",
            // WHERE
            "WHERE",
            // WINDOW
            "WINDOW",
            // WITH
            "WITH",
            // YEAR
            "YEAR",
            // _ROWID_
            "_ROWID_",
            // PARAMETER
            "?",
            // END_OF_INPUT
            null,
            // LITERAL
            null,
            // EQUAL
            "=",
            // BIGGER_EQUAL
            ">=",
            // BIGGER
            ">",
            // SMALLER
            "<",
            // SMALLER_EQUAL
            "<=",
            // NOT_EQUAL
            "<>",
            // AT
            "@",
            // MINUS_SIGN
            "-",
            // PLUS_SIGN
            "+",
            // CONCATENATION
            "||",
            // OPEN_PAREN
            "(",
            // CLOSE_PAREN
            ")",
            // SPATIAL_INTERSECTS
            "&&",
            // ASTERISK
            "*",
            // COMMA
            ",",
            // DOT
            ".",
            // OPEN_BRACE
            "{",
            // CLOSE_BRACE
            "}",
            // SLASH
            "/",
            // PERCENT
            "%",
            // SEMICOLON
            ";",
            // COLON
            ":",
            // OPEN_BRACKET
            "[",
            // CLOSE_BRACKET
            "]",
            // TILDE
            "~",
            // COLON_COLON
            "::",
            // COLON_EQ
            ":=",
            // NOT_TILDE
            "!~",
            // End
    };

    static class IdentifierToken extends Token {

        private String identifier;

        private final boolean quoted;

        private boolean unicode;

        IdentifierToken(int start, String identifier, boolean quoted, boolean unicode) {
            super(start);
            this.identifier = identifier;
            this.quoted = quoted;
            this.unicode = unicode;
        }

        @Override
        int tokenType() {
            return IDENTIFIER;
        }

        @Override
        String asIdentifier() {
            return identifier;
        }

        @Override
        boolean isQuoted() {
            return quoted;
        }

        @Override
        boolean needsUnicodeConversion() {
            return unicode;
        }

        @Override
        void convertUnicode(int uescape) {
            if (unicode) {
                identifier = StringUtils.decodeUnicodeStringSQL(identifier, uescape);
                unicode = false;
            } else {
                throw DbException.getInternalError();
            }
        }

        @Override
        public String toString() {
            return quoted ? StringUtils.quoteIdentifier(identifier) : identifier;
        }

    }

    static final class KeywordToken extends Token {

        private final int type;

        KeywordToken(int start, int type) {
            super(start);
            this.type = type;
        }

        @Override
        int tokenType() {
            return type;
        }

        @Override
        String asIdentifier() {
            return TOKENS[type];
        }

        @Override
        public String toString() {
            return TOKENS[type];
        }

    }

    static final class KeywordOrIdentifierToken extends Token {

        private final int type;

        private final String identifier;

        KeywordOrIdentifierToken(int start, int type, String identifier) {
            super(start);
            this.type = type;
            this.identifier = identifier;
        }

        @Override
        int tokenType() {
            return type;
        }

        @Override
        String asIdentifier() {
            return identifier;
        }

        @Override
        public String toString() {
            return identifier;
        }

    }

    static abstract class LiteralToken extends Token {

        Value value;

        LiteralToken(int start) {
            super(start);
        }

        @Override
        final int tokenType() {
            return LITERAL;
        }

        @Override
        public final String toString() {
            return value(null).getTraceSQL();
        }

    }

    static final class BinaryStringToken extends LiteralToken {

        private final byte[] string;

        BinaryStringToken(int start, byte[] string) {
            super(start);
            this.string = string;
        }

        @Override
        Value value(CastDataProvider provider) {
            if (value == null) {
                value = ValueVarbinary.getNoCopy(string);
            }
            return value;
        }

    }

    static final class CharacterStringToken extends LiteralToken {

        String string;

        private boolean unicode;

        CharacterStringToken(int start, String string, boolean unicode) {
            super(start);
            this.string = string;
            this.unicode = unicode;
        }

        @Override
        Value value(CastDataProvider provider) {
            if (value == null) {
                value = ValueVarchar.get(string, provider);
            }
            return value;
        }

        @Override
        boolean needsUnicodeConversion() {
            return unicode;
        }

        @Override
        void convertUnicode(int uescape) {
            if (unicode) {
                string = StringUtils.decodeUnicodeStringSQL(string, uescape);
                unicode = false;
            } else {
                throw DbException.getInternalError();
            }
        }

    }

    static final class IntegerToken extends LiteralToken {

        private final int number;

        IntegerToken(int start, int number) {
            super(start);
            this.number = number;
        }

        @Override
        Value value(CastDataProvider provider) {
            if (value == null) {
                value = ValueInteger.get(number);
            }
            return value;
        }

    }

    static final class BigintToken extends LiteralToken {

        private final long number;

        BigintToken(int start, long number) {
            super(start);
            this.number = number;
        }

        @Override
        Value value(CastDataProvider provider) {
            if (value == null) {
                value = ValueBigint.get(number);
            }
            return value;
        }

    }

    static final class ValueToken extends LiteralToken {

        ValueToken(int start, Value value) {
            super(start);
            this.value = value;
        }

        @Override
        Value value(CastDataProvider provider) {
            return value;
        }

    }

    static final class ParameterToken extends Token {

        int index;

        ParameterToken(int start, int index) {
            super(start);
            this.index = index;
        }

        @Override
        int tokenType() {
            return PARAMETER;
        }

        @Override
        String asIdentifier() {
            return "?";
        }

        int index() {
            return index;
        }

        @Override
        public String toString() {
            return index == 0 ? "?" : "?" + index;
        }

    }

    static final class EndOfInputToken extends Token {

        EndOfInputToken(int start) {
            super(start);
        }

        @Override
        int tokenType() {
            return END_OF_INPUT;
        }

    }

    private int start;

    Token(int start) {
        this.start = start;
    }

    final int start() {
        return start;
    }

    final void setStart(int offset) {
        start = offset;
    }

    final void subtractFromStart(int offset) {
        start -= offset;
    }

    abstract int tokenType();

    String asIdentifier() {
        return null;
    }

    boolean isQuoted() {
        return false;
    }

    Value value(CastDataProvider provider) {
        return null;
    }

    boolean needsUnicodeConversion() {
        return false;
    }

    void convertUnicode(int uescape) {
        throw DbException.getInternalError();
    }

    @Override
    protected Token clone() {
        try {
            return (Token) super.clone();
        } catch (CloneNotSupportedException e) {
            throw DbException.getInternalError();
        }
    }

}
