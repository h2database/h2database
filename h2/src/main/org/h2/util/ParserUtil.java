/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
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

    /**
     * The token "null".
     */
    public static final int NULL = 3;

    /**
     * The token "true".
     */
    public static final int TRUE = 4;

    /**
     * The token "false".
     */
    public static final int FALSE = 5;

    /**
     * The token "rownum".
     */
    public static final int ROWNUM = 6;

    private ParserUtil() {
        // utility class
    }

    /**
     * Checks if this string is a SQL keyword.
     *
     * @param s the token to check
     * @return true if it is a keyword
     */
    public static boolean isKeyword(String s) {
        if (s == null || s.length() == 0) {
            return false;
        }
        return getSaveTokenType(s, false) != IDENTIFIER;
    }

    /**
     * Is this a simple identifier (in the JDBC specification sense).
     *
     * @param s identifier to check
     * @return is specified identifier may be used without quotes
     * @throws NullPointerException if s is {@code null}
     */
    public static boolean isSimpleIdentifier(String s) {
        if (s.length() == 0) {
            return false;
        }
        char c = s.charAt(0);
        // lowercase a-z is quoted as well
        if ((!Character.isLetter(c) && c != '_') || Character.isLowerCase(c)) {
            return false;
        }
        for (int i = 1, length = s.length(); i < length; i++) {
            c = s.charAt(i);
            if ((!Character.isLetterOrDigit(c) && c != '_') ||
                    Character.isLowerCase(c)) {
                return false;
            }
        }
        return getSaveTokenType(s, true) == IDENTIFIER;
    }

    /**
     * Get the token type.
     *
     * @param s the token
     * @param additionalKeywords whether TOP, INTERSECTS, and "current data /
     *                           time" functions are keywords
     * @return the token type
     */
    public static int getSaveTokenType(String s, boolean additionalKeywords) {
        /*
         * JdbcDatabaseMetaData.getSQLKeywords() and tests should be updated when new
         * non-SQL:2003 keywords are introduced here.
         */
        switch (s.charAt(0)) {
        case 'A':
            return getKeywordOrIdentifier(s, "ALL", KEYWORD);
        case 'C':
            if ("CHECK".equals(s)) {
                return KEYWORD;
            } else if ("CONSTRAINT".equals(s)) {
                return KEYWORD;
            } else if ("CROSS".equals(s)) {
                return KEYWORD;
            }
            if (additionalKeywords) {
                if ("CURRENT_DATE".equals(s) || "CURRENT_TIME".equals(s) || "CURRENT_TIMESTAMP".equals(s)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'D':
            return getKeywordOrIdentifier(s, "DISTINCT", KEYWORD);
        case 'E':
            if ("EXCEPT".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "EXISTS", KEYWORD);
        case 'F':
            if ("FETCH".equals(s)) {
                return KEYWORD;
            } else if ("FROM".equals(s)) {
                return KEYWORD;
            } else if ("FOR".equals(s)) {
                return KEYWORD;
            } else if ("FOREIGN".equals(s)) {
                return KEYWORD;
            } else if ("FULL".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "FALSE", FALSE);
        case 'G':
            return getKeywordOrIdentifier(s, "GROUP", KEYWORD);
        case 'H':
            return getKeywordOrIdentifier(s, "HAVING", KEYWORD);
        case 'I':
            if ("INNER".equals(s) || "INTERSECT".equals(s) || "IS".equals(s)) {
                return KEYWORD;
            }
            if (additionalKeywords) {
                if ("INTERSECTS".equals(s)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'J':
            return getKeywordOrIdentifier(s, "JOIN", KEYWORD);
        case 'L':
            if ("LIMIT".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "LIKE", KEYWORD);
        case 'M':
            return getKeywordOrIdentifier(s, "MINUS", KEYWORD);
        case 'N':
            if ("NOT".equals(s)) {
                return KEYWORD;
            } else if ("NATURAL".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "NULL", NULL);
        case 'O':
            if ("OFFSET".equals(s)) {
                return KEYWORD;
            } else if ("ON".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "ORDER", KEYWORD);
        case 'P':
            return getKeywordOrIdentifier(s, "PRIMARY", KEYWORD);
        case 'R':
            return getKeywordOrIdentifier(s, "ROWNUM", ROWNUM);
        case 'S':
            if ("SELECT".equals(s)) {
                return KEYWORD;
            }
            if (additionalKeywords) {
                if ("SYSDATE".equals(s) || "SYSTIME".equals(s) || "SYSTIMESTAMP".equals(s)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'T':
            if ("TRUE".equals(s)) {
                return TRUE;
            }
            if (additionalKeywords) {
                if ("TODAY".equals(s) || "TOP".equals(s)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'U':
            if ("UNIQUE".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "UNION", KEYWORD);
        case 'W':
            if ("WITH".equals(s)) {
                return KEYWORD;
            }
            return getKeywordOrIdentifier(s, "WHERE", KEYWORD);
        default:
            return IDENTIFIER;
        }
    }

    private static int getKeywordOrIdentifier(String s1, String s2,
            int keywordType) {
        if (s1.equals(s2)) {
            return keywordType;
        }
        return IDENTIFIER;
    }

}
