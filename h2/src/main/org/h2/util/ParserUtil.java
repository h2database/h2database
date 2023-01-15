/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.HashMap;

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
     * The token "INTERVAL".
     */
    public static final int INTERVAL = INTERSECT + 1;

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

    private static final HashMap<String, Integer> KEYWORDS;

    static {
        HashMap<String, Integer> map = new HashMap<>(256);
        map.put("ALL", ALL);
        map.put("AND", AND);
        map.put("ANY", ANY);
        map.put("ARRAY", ARRAY);
        map.put("AS", AS);
        map.put("ASYMMETRIC", ASYMMETRIC);
        map.put("AUTHORIZATION", AUTHORIZATION);
        map.put("BETWEEN", BETWEEN);
        map.put("CASE", CASE);
        map.put("CAST", CAST);
        map.put("CHECK", CHECK);
        map.put("CONSTRAINT", CONSTRAINT);
        map.put("CROSS", CROSS);
        map.put("CURRENT_CATALOG", CURRENT_CATALOG);
        map.put("CURRENT_DATE", CURRENT_DATE);
        map.put("CURRENT_PATH", CURRENT_PATH);
        map.put("CURRENT_ROLE", CURRENT_ROLE);
        map.put("CURRENT_SCHEMA", CURRENT_SCHEMA);
        map.put("CURRENT_TIME", CURRENT_TIME);
        map.put("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP);
        map.put("CURRENT_USER", CURRENT_USER);
        map.put("DAY", DAY);
        map.put("DEFAULT", DEFAULT);
        map.put("DISTINCT", DISTINCT);
        map.put("ELSE", ELSE);
        map.put("END", END);
        map.put("EXCEPT", EXCEPT);
        map.put("EXISTS", EXISTS);
        map.put("FALSE", FALSE);
        map.put("FETCH", FETCH);
        map.put("FOR", FOR);
        map.put("FOREIGN", FOREIGN);
        map.put("FROM", FROM);
        map.put("FULL", FULL);
        map.put("GROUP", GROUP);
        map.put("HAVING", HAVING);
        map.put("HOUR", HOUR);
        map.put("IF", IF);
        map.put("IN", IN);
        map.put("INNER", INNER);
        map.put("INTERSECT", INTERSECT);
        map.put("INTERVAL", INTERVAL);
        map.put("IS", IS);
        map.put("JOIN", JOIN);
        map.put("KEY", KEY);
        map.put("LEFT", LEFT);
        map.put("LIKE", LIKE);
        map.put("LIMIT", LIMIT);
        map.put("LOCALTIME", LOCALTIME);
        map.put("LOCALTIMESTAMP", LOCALTIMESTAMP);
        map.put("MINUS", MINUS);
        map.put("MINUTE", MINUTE);
        map.put("MONTH", MONTH);
        map.put("NATURAL", NATURAL);
        map.put("NOT", NOT);
        map.put("NULL", NULL);
        map.put("OFFSET", OFFSET);
        map.put("ON", ON);
        map.put("OR", OR);
        map.put("ORDER", ORDER);
        map.put("PRIMARY", PRIMARY);
        map.put("QUALIFY", QUALIFY);
        map.put("RIGHT", RIGHT);
        map.put("ROW", ROW);
        map.put("ROWNUM", ROWNUM);
        map.put("SECOND", SECOND);
        map.put("SELECT", SELECT);
        map.put("SESSION_USER", SESSION_USER);
        map.put("SET", SET);
        map.put("SOME", SOME);
        map.put("SYMMETRIC", SYMMETRIC);
        map.put("SYSTEM_USER", SYSTEM_USER);
        map.put("TABLE", TABLE);
        map.put("TO", TO);
        map.put("TRUE", TRUE);
        map.put("UESCAPE", UESCAPE);
        map.put("UNION", UNION);
        map.put("UNIQUE", UNIQUE);
        map.put("UNKNOWN", UNKNOWN);
        map.put("USER", USER);
        map.put("USING", USING);
        map.put("VALUE", VALUE);
        map.put("VALUES", VALUES);
        map.put("WHEN", WHEN);
        map.put("WHERE", WHERE);
        map.put("WINDOW", WINDOW);
        map.put("WITH", WITH);
        map.put("YEAR", YEAR);
        map.put("_ROWID_", _ROWID_);
        // Additional keywords
        map.put("BOTH", KEYWORD);
        map.put("GROUPS", KEYWORD);
        map.put("ILIKE", KEYWORD);
        map.put("LEADING", KEYWORD);
        map.put("OVER", KEYWORD);
        map.put("PARTITION", KEYWORD);
        map.put("RANGE", KEYWORD);
        map.put("REGEXP", KEYWORD);
        map.put("ROWS", KEYWORD);
        map.put("TOP", KEYWORD);
        map.put("TRAILING", KEYWORD);
        KEYWORDS = map;
    }

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
        return getTokenType(s, ignoreCase, false) != IDENTIFIER;
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
        return getTokenType(s, !databaseToUpper, true) == IDENTIFIER;
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
     * @param additionalKeywords
     *            whether context-sensitive keywords are returned as
     *            {@link #KEYWORD}
     * @return the token type
     */
    public static int getTokenType(String s, boolean ignoreCase, boolean additionalKeywords) {
        int length = s.length();
        if (length <= 1 || length > 17) {
            return IDENTIFIER;
        }
        if (ignoreCase) {
            s = StringUtils.toUpperEnglish(s);
        }
        Integer type = KEYWORDS.get(s);
        if (type == null) {
            return IDENTIFIER;
        }
        int t = type;
        return t == KEYWORD && !additionalKeywords ? IDENTIFIER : t;
    }

}
