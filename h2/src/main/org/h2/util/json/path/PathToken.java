/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.math.BigDecimal;

/**
 * SQL/JSON path language token.
 */
public abstract class PathToken {

    /**
     * End of input.
     */
    static final int END_OF_INPUT = 0;

    static final int ASTERISK = END_OF_INPUT + 1;

    static final int AT_SIGN = ASTERISK + 1;

    static final int COMMA = AT_SIGN + 1;

    static final int DOLLAR_SIGN = COMMA + 1;

    static final int DOUBLE_AMPERSAND = DOLLAR_SIGN + 1;

    static final int DOUBLE_EQUALS = DOUBLE_AMPERSAND + 1;

    static final int DOUBLE_VERTICAL_BAR = DOUBLE_EQUALS + 1;

    static final int EXCLAMATION_MARK = DOUBLE_VERTICAL_BAR + 1;

    static final int GREATER_THAN_OPERATOR = EXCLAMATION_MARK + 1;

    static final int GREATER_THAN_OR_EQUALS_OPERATOR = GREATER_THAN_OPERATOR + 1;

    static final int LEFT_BRACKET = GREATER_THAN_OR_EQUALS_OPERATOR + 1;

    static final int LEFT_PAREN = LEFT_BRACKET + 1;

    static final int LESS_THAN_OPERATOR = LEFT_PAREN + 1;

    static final int LESS_THAN_OR_EQUALS_OPERATOR = LESS_THAN_OPERATOR + 1;

    static final int MINUS_SIGN = LESS_THAN_OR_EQUALS_OPERATOR + 1;

    static final int NOT_EQUALS_OPERATOR = MINUS_SIGN + 1;

    static final int PERCENT = NOT_EQUALS_OPERATOR + 1;

    static final int PERIOD = PERCENT + 1;

    static final int PLUS_SIGN = PERIOD + 1;

    static final int QUESTION_MARK = PLUS_SIGN + 1;

    static final int RIGHT_BRACKET = QUESTION_MARK + 1;

    static final int RIGHT_PAREN = RIGHT_BRACKET + 1;

    static final int SOLIDUS = RIGHT_PAREN + 1;

    static final int ABS = SOLIDUS + 1;

    static final int CEILING = ABS + 1;

    static final int DATETIME = CEILING + 1;

    static final int DOUBLE = DATETIME + 1;

    static final int EXISTS = DOUBLE + 1;

    static final int FALSE = EXISTS + 1;

    static final int FLAG = FALSE + 1;

    static final int FLOOR = FLAG + 1;

    static final int IS = FLOOR + 1;

    static final int KEYVALUE = IS + 1;

    static final int LAST = KEYVALUE + 1;

    static final int LAX = LAST + 1;

    static final int LIKE_REGEX = LAX + 1;

    static final int NULL = LIKE_REGEX + 1;

    static final int SIZE = NULL + 1;

    static final int STARTS = SIZE + 1;

    static final int STRICT = STARTS + 1;

    static final int TO = STRICT + 1;

    static final int TRUE = TO + 1;

    static final int TYPE = TRUE + 1;

    static final int UNKNOWN = TYPE + 1;

    static final int WITH = UNKNOWN + 1;

    static final int STRING_LITERAL = WITH + 1;

    static final int NUMERIC_LITERAL = STRING_LITERAL + 1;

    static final int NAMED_VARIABLE = NUMERIC_LITERAL + 1;

    static final int KEY_NAME = NAMED_VARIABLE + 1;

    static final int FIRST_KEYWORD = ABS;

    static final int LAST_KEYWORD = WITH;

    private static final String[] TOKENS = {
            // END_OF_INPUT
            null,
            // ASTERISK
            "*",
            // AT_SIGN
            "@",
            // COMMA
            ",",
            // DOLLAR_SIGN
            "$",
            // DOUBLE_AMPERSAND
            "&&",
            // DOUBLE_EQUALS
            "==",
            // DOUBLE_VERTICAL_BAR
            "||",
            // EXCLAMATION_MARK
            "!",
            // GREATER_THAN_OPERATOR
            ">",
            // GREATER_THAN_OR_EQUALS_OPERATOR
            ">=",
            // LEFT_BRACKET
            "[",
            // LEFT_PAREN ;
            "(",
            // LESS_THAN_OPERATOR
            "<",
            // LESS_THAN_OR_EQUALS_OPERATOR
            "<=",
            // MINUS_SIGN
            "-",
            // NOT_EQUALS_OPERATOR
            "<>",
            // PERCENT
            "%",
            // PERIOD
            ".",
            // PLUS_SIGN
            "+",
            // QUESTION_MARK
            "?",
            // RIGHT_BRACKET
            "]",
            // RIGHT_PAREN
            ")",
            // SOLIDUS
            "/",
            // ABS
            "abs",
            // CEILING
            "ceiling",
            // DATETIME
            "datetime",
            // DOUBLE
            "double",
            // EXISTS
            "exists",
            // FALSE
            "false",
            // FLAG
            "flag",
            // FLOOR
            "floor",
            // IS
            "is",
            // KEYVALUE
            "keyvalue",
            // LAST
            "last",
            // LAX
            "lax",
            // LIKE_REGEX
            "like_regex",
            // NULL
            "null",
            // SIZE
            "size",
            // STARTS
            "starts",
            // STRICT
            "strict",
            // TO
            "to",
            // TRUE
            "true",
            // TYPE
            "type",
            // UNKNOWN
            "unknown",
            // WITH
            "with",
            // STRING_LITERAL
            "\"\"",
            // NUMERIC_LITERAL
            "0",
            // NAMED_VARIABLE
            "$name",
            // KEY_NAME
            "name"
            //
    };

    static class NamedVariableToken extends PathToken {

        private String identifier;

        NamedVariableToken(int start, String identifier) {
            super(start);
            this.identifier = identifier;
        }

        @Override
        int tokenType() {
            return NAMED_VARIABLE;
        }

        @Override
        String asIdentifier() {
            return identifier;
        }

        @Override
        public String toString() {
            return '$' + identifier;
        }

    }

    static class IdentifierToken extends PathToken {

        private String identifier;

        IdentifierToken(int start, String identifier) {
            super(start);
            this.identifier = identifier;
        }

        @Override
        int tokenType() {
            return KEY_NAME;
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

    static final class KeywordToken extends PathToken {

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

    static final class KeywordOrIdentifierToken extends PathToken {

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

    static final class CharacterStringToken extends PathToken {

        private final String string;

        CharacterStringToken(int start, String string) {
            super(start);
            this.string = string;
        }

        @Override
        int tokenType() {
            return STRING_LITERAL;
        }

        @Override
        String asIdentifier() {
            return string;
        }

        @Override
        public String toString() {
            return string;
        }

    }

    static final class IntegerToken extends PathToken {

        private final int number;

        IntegerToken(int start, int number) {
            super(start);
            this.number = number;
        }

        int getNumber() {
            return number;
        }

        @Override
        int tokenType() {
            return NUMERIC_LITERAL;
        }

        @Override
        public String toString() {
            return Integer.toString(number);
        }

    }

    static final class BigDecimalToken extends PathToken {

        private final BigDecimal number;

        BigDecimalToken(int start, BigDecimal number) {
            super(start);
            this.number = number;
        }

        BigDecimal getNumber() {
            return number;
        }

        @Override
        int tokenType() {
            return NUMERIC_LITERAL;
        }

        @Override
        public String toString() {
            String s = number.toString();
            int index = s.indexOf('E');
            if (index >= 0 && s.charAt(++index) == '+') {
                return new StringBuilder().append(s, 0, index).append(s, index + 1, s.length()).toString();
            } else {
                return s;
            }
        }

    }

    static final class EndOfInputToken extends PathToken {

        EndOfInputToken(int start) {
            super(start);
        }

        @Override
        int tokenType() {
            return END_OF_INPUT;
        }

    }

    private int start;

    PathToken(int start) {
        this.start = start;
    }

    final int start() {
        return start;
    }

    String asIdentifier() {
        return null;
    }

    abstract int tokenType();

}
