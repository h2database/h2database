/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;
import java.util.Random;

/**
 * Represents a hard coded terminal rule in a BNF object.
 */
public class RuleFixed implements Rule {
    static final int YMD = 0, HMS = 1, NANOS = 2;
    static final int ANY_EXCEPT_SINGLE_QUOTE = 3;
    static final int ANY_EXCEPT_DOUBLE_QUOTE = 4;
    static final int ANY_UNTIL_EOL = 5;
    static final int ANY_UNTIL_END = 6;
    static final int ANY_WORD = 7;
    static final int ANY_EXCEPT_2_DOLLAR = 8;
    static final int HEX_START = 10, CONCAT = 11, AZ_UNDERSCORE = 12, AF = 13, DIGIT = 14;

    private final int type;

    RuleFixed(int type) {
        this.type = type;
    }

    public String toString() {
        switch(type) {
        case YMD:
            return "2000-01-01";
        case HMS:
            return "12:00";
        case NANOS:
            return "0";
        case ANY_UNTIL_EOL:
        case ANY_EXCEPT_SINGLE_QUOTE:
        case ANY_EXCEPT_DOUBLE_QUOTE:
        case ANY_WORD:
        case ANY_EXCEPT_2_DOLLAR:
        case ANY_UNTIL_END: {
            return "XYZ";
        }
        case HEX_START:
            return "0x";
        case CONCAT:
            return "||";
        case AZ_UNDERSCORE:
            return "A";
        case AF:
            return "F";
        case DIGIT:
            return "0";
        default:
            throw new Error("type="+type);
        }
    }

    public String random(Bnf config, int level) {
        Random r = config.getRandom();
        switch(type) {
        case YMD:
            return "" + (1800 + r.nextInt(200)) + "-" + (1 + r.nextInt(12)) + "-" + (1 + r.nextInt(31));
        case HMS:
            return ""+(r.nextInt(24))+"-"+(r.nextInt(60))+"-"+(r.nextInt(60));
        case NANOS:
            return ""+(r.nextInt(100000)+r.nextInt(10000));
        case ANY_UNTIL_EOL:
        case ANY_EXCEPT_SINGLE_QUOTE:
        case ANY_EXCEPT_DOUBLE_QUOTE:
        case ANY_WORD:
        case ANY_EXCEPT_2_DOLLAR:
        case ANY_UNTIL_END: {
            StringBuilder buff = new StringBuilder();
            int len = r.nextBoolean() ? 1 : r.nextInt(5);
            for (int i = 0; i < len; i++) {
                buff.append((char) ('A' + r.nextInt('C' - 'A')));
            }
            return buff.toString();
        }
        case HEX_START:
            return "0x";
        case CONCAT:
            return "||";
        case AZ_UNDERSCORE:
            return "" + (char) ('A' + r.nextInt('C' - 'A'));
        case AF:
            return "" + (char) ('A' + r.nextInt('F' - 'A'));
        case DIGIT:
            return "" + (char) ('0' + r.nextInt(10));
        default:
            throw new Error("type="+type);
        }
    }

    public String name() {
        return "type="+type;
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // nothing to do
    }

    public boolean matchRemove(Sentence sentence) {
        if (sentence.shouldStop()) {
            return false;
        }
        String query = sentence.getQuery();
        if (query.length() == 0) {
            return false;
        }
        String s = query;
        switch(type) {
        case YMD:
            while (s.length() > 0 && "0123456789- ".indexOf(s.charAt(0)) >= 0) {
                s = s.substring(1);
            }
            break;
        case HMS:
            while (s.length() > 0 && "0123456789:. ".indexOf(s.charAt(0)) >= 0) {
                s = s.substring(1);
            }
            break;
        case NANOS:
            while (s.length() > 0 && Character.isDigit(s.charAt(0))) {
                s = s.substring(1);
            }
            break;
        case ANY_WORD:
            while (s.length() > 0 && Character.isWhitespace(s.charAt(0))) {
                s = s.substring(1);
            }
            break;
        case ANY_UNTIL_END:
            while (s.length() > 1 && s.startsWith("*/")) {
                s = s.substring(1);
            }
            break;
        case ANY_UNTIL_EOL:
            while (s.length() > 0 && s.charAt(0) != '\n') {
                s = s.substring(1);
            }
            break;
        case ANY_EXCEPT_SINGLE_QUOTE:
            while (true) {
                while (s.length() > 0 && s.charAt(0) != '\'') {
                    s = s.substring(1);
                }
                if (s.startsWith("''")) {
                    s = s.substring(2);
                } else {
                    break;
                }
            }
            break;
        case ANY_EXCEPT_DOUBLE_QUOTE:
            while (true) {
                while (s.length() > 0 && s.charAt(0) != '\"') {
                    s = s.substring(1);
                }
                if (s.startsWith("\"\"")) {
                    s = s.substring(2);
                } else {
                    break;
                }
            }
            break;
        case ANY_EXCEPT_2_DOLLAR:
            while (true) {
                while (s.length() > 0 && !s.startsWith("$$")) {
                    s = s.substring(1);
                }
                break;
            }
            break;
        case HEX_START:
            if (s.startsWith("0X") || s.startsWith("0x")) {
                s = s.substring(2);
            } else if (s.startsWith("0")) {
                s = s.substring(1);
            }
            break;
        case CONCAT:
            if (s.startsWith("||")) {
                s = s.substring(2);
            } else if (s.startsWith("|")) {
                s = s.substring(1);
            }
            break;
        case AZ_UNDERSCORE:
            if (s.length() > 0 && (Character.isLetter(s.charAt(0)) || s.charAt(0) == '_')) {
                s = s.substring(1);
            }
            break;
        case AF:
            if (s.length() > 0) {
                char ch = Character.toUpperCase(s.charAt(0));
                if (ch >= 'A' && ch <= 'F') {
                    s = s.substring(1);
                }
            }
            break;
        case DIGIT:
            if (s.length() > 0 && Character.isDigit(s.charAt(0))) {
                s = s.substring(1);
            }
            break;
        default:
            throw new Error("type=" + type);
        }
        if (s.equals(query)) {
            return false;
        }
        sentence.setQuery(s);
        return true;
    }

    public void addNextTokenList(Sentence sentence) {
        if (sentence.shouldStop()) {
            return;
        }
        String query = sentence.getQuery();
        switch(type) {
        case YMD:
            if (query.length() == 0) {
                sentence.add("2006-01-01", "2006-01-01", Sentence.KEYWORD);
            }
            break;
        case HMS:
            if (query.length() == 0) {
                sentence.add("12:00:00", "12:00:00", Sentence.KEYWORD);
            }
            break;
        case NANOS:
            if (query.length() == 0) {
                sentence.add("nanoseconds", "0", Sentence.KEYWORD);
            }
            break;
        case ANY_EXCEPT_SINGLE_QUOTE:
            if (query.length() == 0) {
                sentence.add("anything", "Hello World", Sentence.KEYWORD);
                sentence.add("'", "'", Sentence.KEYWORD);
            }
            break;
        case ANY_EXCEPT_2_DOLLAR:
            if (query.length() == 0) {
                sentence.add("anything", "Hello World", Sentence.KEYWORD);
                sentence.add("'", "'", Sentence.KEYWORD);
            }
            break;
        case ANY_EXCEPT_DOUBLE_QUOTE:
            if (query.length() == 0) {
                sentence.add("anything", "identifier", Sentence.KEYWORD);
            }
            break;
        case ANY_WORD:
            break;
        case HEX_START:
            if (query.length() == 0) {
                sentence.add("0x", "0x", Sentence.KEYWORD);
            } else if ("0".equals(query)) {
                sentence.add("0x", "x", Sentence.KEYWORD);
            }
            break;
        case CONCAT:
            if (query.length() == 0) {
                sentence.add("||", "||", Sentence.KEYWORD);
            } else if ("|".equals(query)) {
                sentence.add("||", "|", Sentence.KEYWORD);
            }
            break;
        case AZ_UNDERSCORE:
            if (query.length() == 0) {
                sentence.add("character", "A", Sentence.KEYWORD);
            }
            break;
        case AF:
            if (query.length() == 0) {
                sentence.add("hex character", "0A", Sentence.KEYWORD);
            }
            break;
        case DIGIT:
            if (query.length() == 0) {
                sentence.add("digit", "1", Sentence.KEYWORD);
            }
            break;
        default:
            throw new Error("type="+type);
        }
    }

}
