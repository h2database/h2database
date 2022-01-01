/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

public class ParserUtil {

    /**
     * A keyword.
     */
    public static final int KEYWORD = 1;

    /**
     * An identifier (table name, column name,...).
     */
    public static final int IDENTIFIER = 2;

    // Constants below must be sorted

    /**
     * The token "ALL".
     */
    public static final int ALL = IDENTIFIER + 1;

    /**
     * The token "AND".
     */
    public static final int AND = ALL + 1;

    /**
     * The token "ANY".
     */
    public static final int ANY = AND + 1;

    /**
     * The token "ARRAY".
     */
    public static final int ARRAY = ANY + 1;

    /**
     * The token "AS".
     */
    public static final int AS = ARRAY + 1;

    /**
     * The token "ASYMMETRIC".
     */
    public static final int ASYMMETRIC = AS + 1;

    /**
     * The token "AUTHORIZATION".
     */
    public static final int AUTHORIZATION = ASYMMETRIC + 1;

    /**
     * The token "BETWEEN".
     */
    public static final int BETWEEN = AUTHORIZATION + 1;

    /**
     * The token "CASE".
     */
    public static final int CASE = BETWEEN + 1;

    /**
     * The token "CAST".
     */
    public static final int CAST = CASE + 1;

    /**
     * The token "CHECK".
     */
    public static final int CHECK = CAST + 1;

    /**
     * The token "CONSTRAINT".
     */
    public static final int CONSTRAINT = CHECK + 1;

    /**
     * The token "CROSS".
     */
    public static final int CROSS = CONSTRAINT + 1;

    /**
     * The token "CURRENT_CATALOG".
     */
    public static final int CURRENT_CATALOG = CROSS + 1;

    /**
     * The token "CURRENT_DATE".
     */
    public static final int CURRENT_DATE = CURRENT_CATALOG + 1;

    /**
     * The token "CURRENT_PATH".
     */
    public static final int CURRENT_PATH = CURRENT_DATE + 1;

    /**
     * The token "CURRENT_ROLE".
     */
    public static final int CURRENT_ROLE = CURRENT_PATH + 1;

    /**
     * The token "CURRENT_SCHEMA".
     */
    public static final int CURRENT_SCHEMA = CURRENT_ROLE + 1;

    /**
     * The token "CURRENT_TIME".
     */
    public static final int CURRENT_TIME = CURRENT_SCHEMA + 1;

    /**
     * The token "CURRENT_TIMESTAMP".
     */
    public static final int CURRENT_TIMESTAMP = CURRENT_TIME + 1;

    /**
     * The token "CURRENT_USER".
     */
    public static final int CURRENT_USER = CURRENT_TIMESTAMP + 1;

    /**
     * The token "DAY".
     */
    public static final int DAY = CURRENT_USER + 1;

    /**
     * The token "DEFAULT".
     */
    public static final int DEFAULT = DAY + 1;

    /**
     * The token "DISTINCT".
     */
    public static final int DISTINCT = DEFAULT + 1;

    /**
     * The token "ELSE".
     */
    public static final int ELSE = DISTINCT + 1;

    /**
     * The token "END".
     */
    public static final int END = ELSE + 1;

    /**
     * The token "EXCEPT".
     */
    public static final int EXCEPT = END + 1;

    /**
     * The token "EXISTS".
     */
    public static final int EXISTS = EXCEPT + 1;

    /**
     * The token "FALSE".
     */
    public static final int FALSE = EXISTS + 1;

    /**
     * The token "FETCH".
     */
    public static final int FETCH = FALSE + 1;

    /**
     * The token "FOR".
     */
    public static final int FOR = FETCH + 1;

    /**
     * The token "FOREIGN".
     */
    public static final int FOREIGN = FOR + 1;

    /**
     * The token "FROM".
     */
    public static final int FROM = FOREIGN + 1;

    /**
     * The token "FULL".
     */
    public static final int FULL = FROM + 1;

    /**
     * The token "GROUP".
     */
    public static final int GROUP = FULL + 1;

    /**
     * The token "HAVING".
     */
    public static final int HAVING = GROUP + 1;

    /**
     * The token "HOUR".
     */
    public static final int HOUR = HAVING + 1;

    /**
     * The token "IF".
     */
    public static final int IF = HOUR + 1;

    /**
     * The token "IN".
     */
    public static final int IN = IF + 1;

    /**
     * The token "INNER".
     */
    public static final int INNER = IN + 1;

    /**
     * The token "INTERSECT".
     */
    public static final int INTERSECT = INNER + 1;

    /**
     * The token "INTERSECTS".
     */
    public static final int INTERSECTS = INTERSECT + 1;

    /**
     * The token "INTERVAL".
     */
    public static final int INTERVAL = INTERSECTS + 1;

    /**
     * The token "IS".
     */
    public static final int IS = INTERVAL + 1;

    /**
     * The token "JOIN".
     */
    public static final int JOIN = IS + 1;

    /**
     * The token "KEY".
     */
    public static final int KEY = JOIN + 1;

    /**
     * The token "LEFT".
     */
    public static final int LEFT = KEY + 1;

    /**
     * The token "LIKE".
     */
    public static final int LIKE = LEFT + 1;

    /**
     * The token "LIMIT".
     */
    public static final int LIMIT = LIKE + 1;

    /**
     * The token "LOCALTIME".
     */
    public static final int LOCALTIME = LIMIT + 1;

    /**
     * The token "LOCALTIMESTAMP".
     */
    public static final int LOCALTIMESTAMP = LOCALTIME + 1;

    /**
     * The token "MINUS".
     */
    public static final int MINUS = LOCALTIMESTAMP + 1;

    /**
     * The token "MINUTE".
     */
    public static final int MINUTE = MINUS + 1;

    /**
     * The token "MONTH".
     */
    public static final int MONTH = MINUTE + 1;

    /**
     * The token "NATURAL".
     */
    public static final int NATURAL = MONTH + 1;

    /**
     * The token "NOT".
     */
    public static final int NOT = NATURAL + 1;

    /**
     * The token "NULL".
     */
    public static final int NULL = NOT + 1;

    /**
     * The token "OFFSET".
     */
    public static final int OFFSET = NULL + 1;

    /**
     * The token "ON".
     */
    public static final int ON = OFFSET + 1;

    /**
     * The token "OR".
     */
    public static final int OR = ON + 1;

    /**
     * The token "ORDER".
     */
    public static final int ORDER = OR + 1;

    /**
     * The token "PRIMARY".
     */
    public static final int PRIMARY = ORDER + 1;

    /**
     * The token "QUALIFY".
     */
    public static final int QUALIFY = PRIMARY + 1;

    /**
     * The token "RIGHT".
     */
    public static final int RIGHT = QUALIFY + 1;

    /**
     * The token "ROW".
     */
    public static final int ROW = RIGHT + 1;

    /**
     * The token "ROWNUM".
     */
    public static final int ROWNUM = ROW + 1;

    /**
     * The token "SECOND".
     */
    public static final int SECOND = ROWNUM + 1;

    /**
     * The token "SELECT".
     */
    public static final int SELECT = SECOND + 1;

    /**
     * The token "SESSION_USER".
     */
    public static final int SESSION_USER = SELECT + 1;

    /**
     * The token "SET".
     */
    public static final int SET = SESSION_USER + 1;

    /**
     * The token "SOME".
     */
    public static final int SOME = SET + 1;

    /**
     * The token "SYMMETRIC".
     */
    public static final int SYMMETRIC = SOME + 1;

    /**
     * The token "SYSTEM_USER".
     */
    public static final int SYSTEM_USER = SYMMETRIC + 1;

    /**
     * The token "TABLE".
     */
    public static final int TABLE = SYSTEM_USER + 1;

    /**
     * The token "TO".
     */
    public static final int TO = TABLE + 1;

    /**
     * The token "TRUE".
     */
    public static final int TRUE = TO + 1;

    /**
     * The token "UESCAPE".
     */
    public static final int UESCAPE = TRUE + 1;

    /**
     * The token "UNION".
     */
    public static final int UNION = UESCAPE + 1;

    /**
     * The token "UNIQUE".
     */
    public static final int UNIQUE = UNION + 1;

    /**
     * The token "UNKNOWN".
     */
    public static final int UNKNOWN = UNIQUE + 1;

    /**
     * The token "USER".
     */
    public static final int USER = UNKNOWN + 1;

    /**
     * The token "USING".
     */
    public static final int USING = USER + 1;

    /**
     * The token "VALUE".
     */
    public static final int VALUE = USING + 1;

    /**
     * The token "VALUES".
     */
    public static final int VALUES = VALUE + 1;

    /**
     * The token "WHEN".
     */
    public static final int WHEN = VALUES + 1;

    /**
     * The token "WHERE".
     */
    public static final int WHERE = WHEN + 1;

    /**
     * The token "WINDOW".
     */
    public static final int WINDOW = WHERE + 1;

    /**
     * The token "WITH".
     */
    public static final int WITH = WINDOW + 1;

    /**
     * The token "YEAR".
     */
    public static final int YEAR = WITH + 1;

    /**
     * The token "_ROWID_".
     */
    public static final int _ROWID_ = YEAR + 1;

    // Constants above must be sorted

    /**
     * The ordinal number of the first keyword.
     */
    public static final int FIRST_KEYWORD = IDENTIFIER + 1;

    /**
     * The ordinal number of the last keyword.
     */
    public static final int LAST_KEYWORD = _ROWID_;

    private ParserUtil() {
        // utility class
    }

    /**
     * Add double quotes around an identifier if required and appends it to the
     * specified string builder.
     *
     * @param builder string builder to append to
     * @param s the identifier
     * @param sqlFlags formatting flags
     * @return the specified builder
     */
    public static StringBuilder quoteIdentifier(StringBuilder builder, String s, int sqlFlags) {
        if (s == null) {
            return builder.append("\"\"");
        }
        if ((sqlFlags & HasSQL.QUOTE_ONLY_WHEN_REQUIRED) != 0 && isSimpleIdentifier(s, false, false)) {
            return builder.append(s);
        }
        return StringUtils.quoteIdentifier(builder, s);
    }

    /**
     * Checks if this string is a SQL keyword.
     *
     * @param s the token to check
     * @param ignoreCase true if case should be ignored, false if only upper case
     *            tokens are detected as keywords
     * @return true if it is a keyword
     */
    public static boolean isKeyword(String s, boolean ignoreCase) {
        return getTokenType(s, ignoreCase, 0, s.length(), false) != IDENTIFIER;
    }

    /**
     * Is this a simple identifier (in the JDBC specification sense).
     *
     * @param s identifier to check
     * @param databaseToUpper whether unquoted identifiers are converted to upper case
     * @param databaseToLower whether unquoted identifiers are converted to lower case
     * @return is specified identifier may be used without quotes
     * @throws NullPointerException if s is {@code null}
     */
    public static boolean isSimpleIdentifier(String s, boolean databaseToUpper, boolean databaseToLower) {
        if (databaseToUpper && databaseToLower) {
            throw new IllegalArgumentException("databaseToUpper && databaseToLower");
        }
        int length = s.length();
        if (length == 0 || !checkLetter(databaseToUpper, databaseToLower, s.charAt(0))) {
            return false;
        }
        for (int i = 1; i < length; i++) {
            char c = s.charAt(i);
            if (c != '_' && (c < '0' || c > '9') && !checkLetter(databaseToUpper, databaseToLower, c)) {
                return false;
            }
        }
        return getTokenType(s, !databaseToUpper, 0, length, true) == IDENTIFIER;
    }

    private static boolean checkLetter(boolean databaseToUpper, boolean databaseToLower, char c) {
        if (databaseToUpper) {
            if (c < 'A' || c > 'Z') {
                return false;
            }
        } else if (databaseToLower) {
            if (c < 'a' || c > 'z') {
                return false;
            }
        } else {
            if ((c < 'A' || c > 'Z') && (c < 'a' || c > 'z')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the token type.
     *
     * @param s the string with token
     * @param ignoreCase true if case should be ignored, false if only upper case
     *            tokens are detected as keywords
     * @param start start index of token
     * @param length length of token; must be positive
     * @param additionalKeywords
     *            whether context-sensitive keywords are returned as
     *            {@link #KEYWORD}
     * @return the token type
     */
    public static int getTokenType(String s, boolean ignoreCase, int start, int length, boolean additionalKeywords) {
        if (length <= 1 || length > 17) {
            return IDENTIFIER;
        }
        /*
         * DatabaseMetaLocal.getSQLKeywords() and tests should be updated when new
         * non-SQL:2003 keywords are introduced here.
         */
        char c1 = s.charAt(start);
        if (ignoreCase) {
            // Convert a-z to A-Z and 0x7f to _ (need special handling).
            c1 &= 0xffdf;
        }
        if (length == 2) {
            char c2 = s.charAt(start + 1);
            if (ignoreCase) {
                c2 &= 0xffdf;
            }
            switch (c1) {
            case 'A':
                if (c2 == 'S') {
                    return AS;
                }
                return IDENTIFIER;
            case 'I':
                if (c2 == 'F') {
                    return IF;
                } else if (c2 == 'N') {
                    return IN;
                } else if (c2 == 'S') {
                    return IS;
                }
                return IDENTIFIER;
            case 'O':
                if (c2 == 'N') {
                    return ON;
                } else if (c2 == 'R') {
                    return OR;
                }
                return IDENTIFIER;
            case 'T':
                if (c2 == 'O') {
                    return TO;
                }
                //$FALL-THROUGH$
            default:
                return IDENTIFIER;
            }
        }
        switch (c1) {
        case 'A':
            if (eq("ALL", s, ignoreCase, start, length)) {
                return ALL;
            } else if (eq("AND", s, ignoreCase, start, length)) {
                return AND;
            } else if (eq("ANY", s, ignoreCase, start, length)) {
                return ANY;
            } else if (eq("ARRAY", s, ignoreCase, start, length)) {
                return ARRAY;
            } else if (eq("ASYMMETRIC", s, ignoreCase, start, length)) {
                return ASYMMETRIC;
            } else if (eq("AUTHORIZATION", s, ignoreCase, start, length)) {
                return AUTHORIZATION;
            }
            return IDENTIFIER;
        case 'B':
            if (eq("BETWEEN", s, ignoreCase, start, length)) {
                return BETWEEN;
            }
            if (additionalKeywords) {
                if (eq("BOTH", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'C':
            if (eq("CASE", s, ignoreCase, start, length)) {
                return CASE;
            } else if (eq("CAST", s, ignoreCase, start, length)) {
                return CAST;
            } else if (eq("CHECK", s, ignoreCase, start, length)) {
                return CHECK;
            } else if (eq("CONSTRAINT", s, ignoreCase, start, length)) {
                return CONSTRAINT;
            } else if (eq("CROSS", s, ignoreCase, start, length)) {
                return CROSS;
            } else if (length >= 12 && "CURRENT_".regionMatches(ignoreCase, 1, s, start + 1, 7)) {
                return getTokenTypeCurrent(s, ignoreCase, start, length);
            }
            return IDENTIFIER;
        case 'D':
            if (eq("DAY", s, ignoreCase, start, length)) {
                return DAY;
            } else if (eq("DEFAULT", s, ignoreCase, start, length)) {
                return DEFAULT;
            } else if (eq("DISTINCT", s, ignoreCase, start, length)) {
                return DISTINCT;
            }
            return IDENTIFIER;
        case 'E':
            if (eq("ELSE", s, ignoreCase, start, length)) {
                return ELSE;
            } else if (eq("END", s, ignoreCase, start, length)) {
                return END;
            } else if (eq("EXCEPT", s, ignoreCase, start, length)) {
                return EXCEPT;
            } else if (eq("EXISTS", s, ignoreCase, start, length)) {
                return EXISTS;
            }
            return IDENTIFIER;
        case 'F':
            if (eq("FETCH", s, ignoreCase, start, length)) {
                return FETCH;
            } else if (eq("FROM", s, ignoreCase, start, length)) {
                return FROM;
            } else if (eq("FOR", s, ignoreCase, start, length)) {
                return FOR;
            } else if (eq("FOREIGN", s, ignoreCase, start, length)) {
                return FOREIGN;
            } else if (eq("FULL", s, ignoreCase, start, length)) {
                return FULL;
            } else if (eq("FALSE", s, ignoreCase, start, length)) {
                return FALSE;
            }
            if (additionalKeywords) {
                if (eq("FILTER", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'G':
            if (eq("GROUP", s, ignoreCase, start, length)) {
                return GROUP;
            }
            if (additionalKeywords) {
                if (eq("GROUPS", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'H':
            if (eq("HAVING", s, ignoreCase, start, length)) {
                return HAVING;
            } else if (eq("HOUR", s, ignoreCase, start, length)) {
                return HOUR;
            }
            return IDENTIFIER;
        case 'I':
            if (eq("INNER", s, ignoreCase, start, length)) {
                return INNER;
            } else if (eq("INTERSECT", s, ignoreCase, start, length)) {
                return INTERSECT;
            } else if (eq("INTERSECTS", s, ignoreCase, start, length)) {
                return INTERSECTS;
            } else if (eq("INTERVAL", s, ignoreCase, start, length)) {
                return INTERVAL;
            }
            if (additionalKeywords) {
                if (eq("ILIKE", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'J':
            if (eq("JOIN", s, ignoreCase, start, length)) {
                return JOIN;
            }
            return IDENTIFIER;
        case 'K':
            if (eq("KEY", s, ignoreCase, start, length)) {
                return KEY;
            }
            return IDENTIFIER;
        case 'L':
            if (eq("LEFT", s, ignoreCase, start, length)) {
                return LEFT;
            } else if (eq("LIMIT", s, ignoreCase, start, length)) {
                return LIMIT;
            } else if (eq("LIKE", s, ignoreCase, start, length)) {
                return LIKE;
            } else if (eq("LOCALTIME", s, ignoreCase, start, length)) {
                return LOCALTIME;
            } else if (eq("LOCALTIMESTAMP", s, ignoreCase, start, length)) {
                return LOCALTIMESTAMP;
            }
            if (additionalKeywords) {
                if (eq("LEADING", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'M':
            if (eq("MINUS", s, ignoreCase, start, length)) {
                return MINUS;
            } else if (eq("MINUTE", s, ignoreCase, start, length)) {
                return MINUTE;
            } else if (eq("MONTH", s, ignoreCase, start, length)) {
                return MONTH;
            }
            return IDENTIFIER;
        case 'N':
            if (eq("NOT", s, ignoreCase, start, length)) {
                return NOT;
            } else if (eq("NATURAL", s, ignoreCase, start, length)) {
                return NATURAL;
            } else if (eq("NULL", s, ignoreCase, start, length)) {
                return NULL;
            }
            return IDENTIFIER;
        case 'O':
            if (eq("OFFSET", s, ignoreCase, start, length)) {
                return OFFSET;
            } else if (eq("ORDER", s, ignoreCase, start, length)) {
                return ORDER;
            }
            if (additionalKeywords) {
                if (eq("OVER", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'P':
            if (eq("PRIMARY", s, ignoreCase, start, length)) {
                return PRIMARY;
            }
            if (additionalKeywords) {
                if (eq("PARTITION", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'Q':
            if (eq("QUALIFY", s, ignoreCase, start, length)) {
                return QUALIFY;
            }
            return IDENTIFIER;
        case 'R':
            if (eq("RIGHT", s, ignoreCase, start, length)) {
                return RIGHT;
            } else if (eq("ROW", s, ignoreCase, start, length)) {
                return ROW;
            } else if (eq("ROWNUM", s, ignoreCase, start, length)) {
                return ROWNUM;
            }
            if (additionalKeywords) {
                if (eq("RANGE", s, ignoreCase, start, length) || eq("REGEXP", s, ignoreCase, start, length)
                        || eq("ROWS", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'S':
            if (eq("SECOND", s, ignoreCase, start, length)) {
                return SECOND;
            } else if (eq("SELECT", s, ignoreCase, start, length)) {
                return SELECT;
            } else if (eq("SESSION_USER", s, ignoreCase, start, length)) {
                return SESSION_USER;
            } else if (eq("SET", s, ignoreCase, start, length)) {
                return SET;
            } else if (eq("SOME", s, ignoreCase, start, length)) {
                return SOME;
            } else if (eq("SYMMETRIC", s, ignoreCase, start, length)) {
                return SYMMETRIC;
            } else if (eq("SYSTEM_USER", s, ignoreCase, start, length)) {
                return SYSTEM_USER;
            }
            return IDENTIFIER;
        case 'T':
            if (eq("TABLE", s, ignoreCase, start, length)) {
                return TABLE;
            } else if (eq("TRUE", s, ignoreCase, start, length)) {
                return TRUE;
            }
            if (additionalKeywords) {
                if (eq("TOP", s, ignoreCase, start, length) || eq("TRAILING", s, ignoreCase, start, length)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'U':
            if (eq("UESCAPE", s, ignoreCase, start, length)) {
                return UESCAPE;
            } else if (eq("UNION", s, ignoreCase, start, length)) {
                return UNION;
            } else if (eq("UNIQUE", s, ignoreCase, start, length)) {
                return UNIQUE;
            } else if (eq("UNKNOWN", s, ignoreCase, start, length)) {
                return UNKNOWN;
            } else if (eq("USER", s, ignoreCase, start, length)) {
                return USER;
            } else if (eq("USING", s, ignoreCase, start, length)) {
                return USING;
            }
            return IDENTIFIER;
        case 'V':
            if (eq("VALUE", s, ignoreCase, start, length)) {
                return VALUE;
            } else if (eq("VALUES", s, ignoreCase, start, length)) {
                return VALUES;
            }
            return IDENTIFIER;
        case 'W':
            if (eq("WHEN", s, ignoreCase, start, length)) {
                return WHEN;
            } else if (eq("WHERE", s, ignoreCase, start, length)) {
                return WHERE;
            } else if (eq("WINDOW", s, ignoreCase, start, length)) {
                return WINDOW;
            } else if (eq("WITH", s, ignoreCase, start, length)) {
                return WITH;
            }
            return IDENTIFIER;
        case 'Y':
            if (eq("YEAR", s, ignoreCase, start, length)) {
                return YEAR;
            }
            return IDENTIFIER;
        case '_':
            // Cannot use eq() because 0x7f can be converted to '_' (0x5f)
            if (length == 7 && "_ROWID_".regionMatches(ignoreCase, 0, s, start, 7)) {
                return _ROWID_;
            }
            //$FALL-THROUGH$
        default:
            return IDENTIFIER;
        }
    }

    private static boolean eq(String expected, String s, boolean ignoreCase, int start, int length) {
        // First letter was already checked
        return length == expected.length() && expected.regionMatches(ignoreCase, 1, s, start + 1, length - 1);
    }

    private static int getTokenTypeCurrent(String s, boolean ignoreCase, int start, int length) {
        start += 8;
        switch (length -= 8) {
        case 4:
            if ("CURRENT_DATE".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_DATE;
            } else if ("CURRENT_PATH".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_PATH;
            } else if ("CURRENT_ROLE".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_ROLE;
            } else if ("CURRENT_TIME".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_TIME;
            } else if ("CURRENT_USER".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_USER;
            }
            break;
        case 6:
            if ("CURRENT_SCHEMA".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_SCHEMA;
            }
            break;
        case 7:
            if ("CURRENT_CATALOG".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_CATALOG;
            }
            break;
        case 9:
            if ("CURRENT_TIMESTAMP".regionMatches(ignoreCase, 8, s, start, length)) {
                return CURRENT_TIMESTAMP;
            }
        }
        return IDENTIFIER;
    }

}
