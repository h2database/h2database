/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

/**
 * Represents a hard coded terminal rule in a BNF object.
 */
public class RuleFixed implements Rule {

    public static final int YMD = 0;
    public static final int HMS = YMD + 1;
    public static final int NANOS = HMS + 1;
    public static final int ANY_EXCEPT_SINGLE_QUOTE = NANOS + 1;
    public static final int ANY_EXCEPT_DOUBLE_QUOTE = ANY_EXCEPT_SINGLE_QUOTE + 1;
    public static final int ANY_UNTIL_EOL = ANY_EXCEPT_DOUBLE_QUOTE + 1;
    public static final int ANY_UNTIL_END = ANY_UNTIL_EOL + 1;
    public static final int ANY_WORD = ANY_UNTIL_END + 1;
    public static final int ANY_EXCEPT_2_DOLLAR = ANY_WORD + 1;
    public static final int HEX_START = ANY_EXCEPT_2_DOLLAR + 1;
    public static final int OCTAL_START = HEX_START + 1;
    public static final int BINARY_START = OCTAL_START + 1;
    public static final int CONCAT = BINARY_START + 1;
    public static final int AZ_UNDERSCORE = CONCAT + 1;
    public static final int AF = AZ_UNDERSCORE + 1;
    public static final int DIGIT = AF + 1;
    public static final int OPEN_BRACKET = DIGIT + 1;
    public static final int CLOSE_BRACKET = OPEN_BRACKET + 1;
    public static final int JSON_TEXT = CLOSE_BRACKET + 1;

    private final int type;

    RuleFixed(int type) {
        this.type = type;
    }

    @Override
    public void accept(BnfVisitor visitor) {
        visitor.visitRuleFixed(type);
    }

    @Override
    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        // nothing to do
    }

    @Override
    public boolean autoComplete(Sentence sentence) {
        sentence.stopIfRequired();
        String query = sentence.getQuery();
        String s = query;
        boolean removeTrailingSpaces = false;
        switch (type) {
        case YMD:
            while (s.length() > 0 && "0123456789-".indexOf(s.charAt(0)) >= 0) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("2006-01-01", "1", Sentence.KEYWORD);
            }
            // needed for timestamps
            removeTrailingSpaces = true;
            break;
        case HMS:
            while (s.length() > 0 && "0123456789:".indexOf(s.charAt(0)) >= 0) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("12:00:00", "1", Sentence.KEYWORD);
            }
            break;
        case NANOS:
            while (s.length() > 0 && Character.isDigit(s.charAt(0))) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("nanoseconds", "0", Sentence.KEYWORD);
            }
            removeTrailingSpaces = true;
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
            if (s.length() == 0) {
                sentence.add("anything", "Hello World", Sentence.KEYWORD);
                sentence.add("'", "'", Sentence.KEYWORD);
            }
            break;
        case ANY_EXCEPT_2_DOLLAR:
            while (s.length() > 0 && !s.startsWith("$$")) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("anything", "Hello World", Sentence.KEYWORD);
                sentence.add("$$", "$$", Sentence.KEYWORD);
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
            if (s.length() == 0) {
                sentence.add("anything", "identifier", Sentence.KEYWORD);
                sentence.add("\"", "\"", Sentence.KEYWORD);
            }
            break;
        case ANY_WORD:
        case JSON_TEXT:
            while (s.length() > 0 && !Bnf.startWithSpace(s)) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("anything", "anything", Sentence.KEYWORD);
            }
            break;
        case HEX_START:
            if (s.startsWith("0X") || s.startsWith("0x")) {
                s = s.substring(2);
            } else if ("0".equals(s)) {
                sentence.add("0x", "x", Sentence.KEYWORD);
            } else if (s.length() == 0) {
                sentence.add("0x", "0x", Sentence.KEYWORD);
            }
            break;
        case OCTAL_START:
            if (s.startsWith("0O") || s.startsWith("0o")) {
                s = s.substring(2);
            } else if ("0".equals(s)) {
                sentence.add("0o", "o", Sentence.KEYWORD);
            } else if (s.length() == 0) {
                sentence.add("0o", "0o", Sentence.KEYWORD);
            }
            break;
        case BINARY_START:
            if (s.startsWith("0B") || s.startsWith("0b")) {
                s = s.substring(2);
            } else if ("0".equals(s)) {
                sentence.add("0b", "b", Sentence.KEYWORD);
            } else if (s.length() == 0) {
                sentence.add("0b", "0b", Sentence.KEYWORD);
            }
            break;
        case CONCAT:
            if (s.equals("|")) {
                sentence.add("||", "|", Sentence.KEYWORD);
            } else if (s.startsWith("||")) {
                s = s.substring(2);
            } else if (s.length() == 0) {
                sentence.add("||", "||", Sentence.KEYWORD);
            }
            removeTrailingSpaces = true;
            break;
        case AZ_UNDERSCORE:
            if (s.length() > 0 &&
                    (Character.isLetter(s.charAt(0)) || s.charAt(0) == '_')) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("character", "A", Sentence.KEYWORD);
            }
            break;
        case AF:
            if (s.length() > 0) {
                char ch = Character.toUpperCase(s.charAt(0));
                if (ch >= 'A' && ch <= 'F') {
                    s = s.substring(1);
                }
            }
            if (s.length() == 0) {
                sentence.add("hex character", "0A", Sentence.KEYWORD);
            }
            break;
        case DIGIT:
            if (s.length() > 0 && Character.isDigit(s.charAt(0))) {
                s = s.substring(1);
            }
            if (s.length() == 0) {
                sentence.add("digit", "1", Sentence.KEYWORD);
            }
            break;
        case OPEN_BRACKET:
            if (s.length() == 0) {
                sentence.add("[", "[", Sentence.KEYWORD);
            } else if (s.charAt(0) == '[') {
                s = s.substring(1);
            }
            removeTrailingSpaces = true;
            break;
        case CLOSE_BRACKET:
            if (s.length() == 0) {
                sentence.add("]", "]", Sentence.KEYWORD);
            } else if (s.charAt(0) == ']') {
                s = s.substring(1);
            }
            removeTrailingSpaces = true;
            break;
        // no autocomplete support for comments
        // (comments are not reachable in the bnf tree)
        case ANY_UNTIL_EOL:
        case ANY_UNTIL_END:
        default:
            throw new AssertionError("type="+type);
        }
        if (!s.equals(query)) {
            // can not always remove spaces here, because a repeat
            // rule for a-z would remove multiple words
            // but we have to remove spaces after '||'
            // and after ']'
            if (removeTrailingSpaces) {
                while (Bnf.startWithSpace(s)) {
                    s = s.substring(1);
                }
            }
            sentence.setQuery(s);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "#" + type;
    }

}
