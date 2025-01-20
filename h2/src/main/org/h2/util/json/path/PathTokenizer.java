/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.ABS;
import static org.h2.util.json.path.PathToken.ASTERISK;
import static org.h2.util.json.path.PathToken.AT_SIGN;
import static org.h2.util.json.path.PathToken.CEILING;
import static org.h2.util.json.path.PathToken.COMMA;
import static org.h2.util.json.path.PathToken.DATETIME;
import static org.h2.util.json.path.PathToken.DOLLAR_SIGN;
import static org.h2.util.json.path.PathToken.DOUBLE;
import static org.h2.util.json.path.PathToken.DOUBLE_AMPERSAND;
import static org.h2.util.json.path.PathToken.DOUBLE_EQUALS;
import static org.h2.util.json.path.PathToken.DOUBLE_VERTICAL_BAR;
import static org.h2.util.json.path.PathToken.EXCLAMATION_MARK;
import static org.h2.util.json.path.PathToken.EXISTS;
import static org.h2.util.json.path.PathToken.FALSE;
import static org.h2.util.json.path.PathToken.FLAG;
import static org.h2.util.json.path.PathToken.FLOOR;
import static org.h2.util.json.path.PathToken.GREATER_THAN_OPERATOR;
import static org.h2.util.json.path.PathToken.GREATER_THAN_OR_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.IS;
import static org.h2.util.json.path.PathToken.KEYVALUE;
import static org.h2.util.json.path.PathToken.KEY_NAME;
import static org.h2.util.json.path.PathToken.LAST;
import static org.h2.util.json.path.PathToken.LAX;
import static org.h2.util.json.path.PathToken.LEFT_BRACKET;
import static org.h2.util.json.path.PathToken.LEFT_PAREN;
import static org.h2.util.json.path.PathToken.LESS_THAN_OPERATOR;
import static org.h2.util.json.path.PathToken.LESS_THAN_OR_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.LIKE_REGEX;
import static org.h2.util.json.path.PathToken.MINUS_SIGN;
import static org.h2.util.json.path.PathToken.NOT_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.NULL;
import static org.h2.util.json.path.PathToken.PERCENT;
import static org.h2.util.json.path.PathToken.PERIOD;
import static org.h2.util.json.path.PathToken.PLUS_SIGN;
import static org.h2.util.json.path.PathToken.QUESTION_MARK;
import static org.h2.util.json.path.PathToken.RIGHT_BRACKET;
import static org.h2.util.json.path.PathToken.RIGHT_PAREN;
import static org.h2.util.json.path.PathToken.SIZE;
import static org.h2.util.json.path.PathToken.SOLIDUS;
import static org.h2.util.json.path.PathToken.STARTS;
import static org.h2.util.json.path.PathToken.STRICT;
import static org.h2.util.json.path.PathToken.TO;
import static org.h2.util.json.path.PathToken.TRUE;
import static org.h2.util.json.path.PathToken.TYPE;
import static org.h2.util.json.path.PathToken.UNKNOWN;
import static org.h2.util.json.path.PathToken.WITH;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;

import org.h2.message.DbException;

/**
 * SQL/JSON path tokenizer.
 */
final class PathTokenizer {

    /**
     * Constructs tokens from the specified SQL/JSON path.
     *
     * @param path
     *            SQL/JSON path
     * @return tokens
     */
    static ArrayList<PathToken> tokenize(String path) {
        ArrayList<PathToken> tokens = new ArrayList<>();
        int end = path.length() - 1;
        loop: for (int i = 0; i <= end;) {
            char c = path.charAt(i);
            PathToken token;
            switch (c) {
            case '!':
                if (i < end) {
                    char c2 = path.charAt(i + 1);
                    if (c2 == '=') {
                        token = new PathToken.KeywordToken(i++, NOT_EQUALS_OPERATOR);
                        break;
                    }
                }
                token = new PathToken.KeywordToken(i, EXCLAMATION_MARK);
                break;
            case '"':
            case '\'':
                i = readCharacterString(path, i, end, i, c, tokens);
                continue loop;
            case '$':
                if (i < end) {
                    int cp = path.codePointAt(i + 1);
                    if (Character.isJavaIdentifierPart(cp)) {
                        i++;
                        int endIndex = findIdentifierEnd(path, end, i + Character.charCount(cp));
                        tokens.add(new PathToken.NamedVariableToken(i, extractIdentifier(path, i, endIndex)));
                        i = endIndex;
                        continue loop;
                    }
                }
                token = new PathToken.KeywordToken(i, DOLLAR_SIGN);
                break;
            case '%':
                token = new PathToken.KeywordToken(i, PERCENT);
                break;
            case '&':
                if (i < end) {
                    char c2 = path.charAt(i + 1);
                    if (c2 == '&') {
                        token = new PathToken.KeywordToken(i++, DOUBLE_AMPERSAND);
                        break;
                    }
                }
                throw DbException.getSyntaxError(path, i);
            case '(':
                token = new PathToken.KeywordToken(i, LEFT_PAREN);
                break;
            case ')':
                token = new PathToken.KeywordToken(i, RIGHT_PAREN);
                break;
            case '*':
                token = new PathToken.KeywordToken(i, ASTERISK);
                break;
            case '+':
                token = new PathToken.KeywordToken(i, PLUS_SIGN);
                break;
            case ',':
                token = new PathToken.KeywordToken(i, COMMA);
                break;
            case '-':
                token = new PathToken.KeywordToken(i, MINUS_SIGN);
                break;
            case '.':
                token = new PathToken.KeywordToken(i, PERIOD);
                break;
            case '/':
                token = new PathToken.KeywordToken(i, SOLIDUS);
                break;
            case '0':
                if (i < end) {
                    switch (path.charAt(i + 1) & 0xffdf) {
                    case 'B':
                        i = readIntegerNumber(path, i, end, i + 2, tokens, "Binary number", 2);
                        continue loop;
                    case 'O':
                        i = readIntegerNumber(path, i, end, i + 2, tokens, "Octal number", 8);
                        continue loop;
                    case 'X':
                        i = readIntegerNumber(path, i, end, i + 2, tokens, "Hex number", 16);
                        continue loop;
                    }
                }
                //$FALL-THROUGH$
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                i = readNumeric(path, i, end, i + 1, c, tokens);
                continue loop;
            case '<':
                if (i < end) {
                    char c2 = path.charAt(i + 1);
                    if (c2 == '=') {
                        token = new PathToken.KeywordToken(i++, LESS_THAN_OR_EQUALS_OPERATOR);
                        break;
                    }
                    if (c2 == '>') {
                        token = new PathToken.KeywordToken(i++, NOT_EQUALS_OPERATOR);
                        break;
                    }
                }
                token = new PathToken.KeywordToken(i, LESS_THAN_OPERATOR);
                break;
            case '=':
                if (i < end && path.charAt(i + 1) == '=') {
                    token = new PathToken.KeywordToken(i++, DOUBLE_EQUALS);
                    break;
                }
                throw DbException.getSyntaxError(path, i);
            case '>':
                if (i < end && path.charAt(i + 1) == '=') {
                    token = new PathToken.KeywordToken(i++, GREATER_THAN_OR_EQUALS_OPERATOR);
                    break;
                }
                token = new PathToken.KeywordToken(i, GREATER_THAN_OPERATOR);
                break;
            case '?':
                token = new PathToken.KeywordToken(i, QUESTION_MARK);
                break;
            case '@':
                token = new PathToken.KeywordToken(i, AT_SIGN);
                break;
            case '[':
                token = new PathToken.KeywordToken(i, LEFT_BRACKET);
                break;
            case ']':
                token = new PathToken.KeywordToken(i, RIGHT_BRACKET);
                break;
            case 'a':
                i = readA(path, end, i, tokens);
                continue loop;
            case 'c':
                i = readC(path, end, i, tokens);
                continue loop;
            case 'd':
                i = readD(path, end, i, tokens);
                continue loop;
            case 'e':
                i = readE(path, end, i, tokens);
                continue loop;
            case 'f':
                i = readF(path, end, i, tokens);
                continue loop;
            case 'i':
                i = readI(path, end, i, tokens);
                continue loop;
            case 'k':
                i = readK(path, end, i, tokens);
                continue loop;
            case 'l':
                i = readL(path, end, i, tokens);
                continue loop;
            case 'n':
                i = readN(path, end, i, tokens);
                continue loop;
            case 's':
                i = readS(path, end, i, tokens);
                continue loop;
            case 't':
                i = readT(path, end, i, tokens);
                continue loop;
            case 'u':
                i = readU(path, end, i, tokens);
                continue loop;
            case 'w':
                i = readW(path, end, i, tokens);
                continue loop;
            case '|':
                if (i < end) {
                    char c2 = path.charAt(i + 1);
                    if (c2 == '|') {
                        token = new PathToken.KeywordToken(i++, DOUBLE_VERTICAL_BAR);
                        break;
                    }
                }
                throw DbException.getSyntaxError(path, i);
            default:
                if (c <= ' ') {
                    i++;
                    continue loop;
                } else {
                    int tokenStart = i;
                    int cp = Character.isHighSurrogate(c) ? path.codePointAt(i++) : c;
                    if (Character.isSpaceChar(cp)) {
                        i++;
                        continue loop;
                    }
                    if (Character.isJavaIdentifierStart(cp)) {
                        i = readIdentifier(path, end, tokenStart, i, tokens);
                        continue loop;
                    }
                    throw DbException.getSyntaxError(path, tokenStart);
                }
            }
            tokens.add(token);
            i++;
        }
        tokens.add(new PathToken.EndOfInputToken(end + 1));
        return tokens;
    }

    private static int readIdentifier(String path, int end, int tokenStart, int i, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, i);
        tokens.add(new PathToken.IdentifierToken(tokenStart, extractIdentifier(path, tokenStart, endIndex)));
        return endIndex;
    }

    private static int readA(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("abs", path, tokenStart, length)) {
            type = ABS;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readC(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("ceiling", path, tokenStart, length)) {
            type = CEILING;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readD(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("datetime", path, tokenStart, length)) {
            type = DATETIME;
        } else if (eq("double", path, tokenStart, length)) {
            type = DOUBLE;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readE(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("exists", path, tokenStart, length)) {
            type = EXISTS;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readF(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("false", path, tokenStart, length)) {
            type = FALSE;
        } else if (eq("flag", path, tokenStart, length)) {
            type = FLAG;
        } else if (eq("floor", path, tokenStart, length)) {
            type = FLOOR;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readI(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("is", path, tokenStart, length)) {
            type = IS;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readK(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("keyvalue", path, tokenStart, length)) {
            type = KEYVALUE;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readL(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("last", path, tokenStart, length)) {
            type = LAST;
        } else if (eq("lax", path, tokenStart, length)) {
            type = LAX;
        } else if (eq("like_regex", path, tokenStart, length)) {
            type = LIKE_REGEX;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readN(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("null", path, tokenStart, length)) {
            type = NULL;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readS(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("size", path, tokenStart, length)) {
            type = SIZE;
        } else if (eq("starts", path, tokenStart, length)) {
            type = STARTS;
        } else if (eq("strict", path, tokenStart, length)) {
            type = STRICT;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readT(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("to", path, tokenStart, length)) {
            type = TO;
        } else if (eq("true", path, tokenStart, length)) {
            type = TRUE;
        } else if (eq("type", path, tokenStart, length)) {
            type = TYPE;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readU(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("unknown", path, tokenStart, length)) {
            type = UNKNOWN;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readW(String path, int end, int tokenStart, ArrayList<PathToken> tokens) {
        int endIndex = findIdentifierEnd(path, end, tokenStart);
        int length = endIndex - tokenStart;
        int type;
        if (eq("with", path, tokenStart, length)) {
            type = WITH;
        } else {
            type = KEY_NAME;
        }
        return readIdentifierOrKeyword(path, tokenStart, tokens, endIndex, type);
    }

    private static int readIdentifierOrKeyword(String path, int tokenStart, ArrayList<PathToken> tokens, int endIndex,
            int type) {
        PathToken token;
        if (type == KEY_NAME) {
            token = new PathToken.IdentifierToken(tokenStart, extractIdentifier(path, tokenStart, endIndex));
        } else {
            token = new PathToken.KeywordOrIdentifierToken(tokenStart, type,
                    extractIdentifier(path, tokenStart, endIndex));
        }
        tokens.add(token);
        return endIndex;
    }

    private static boolean eq(String expected, String s, int start, int length) {
        return length == expected.length() && s.regionMatches(start + 1, expected, 1, length - 1);
    }

    private static int findIdentifierEnd(String path, int end, int i) {
        for (;;) {
            int cp;
            if (i > end || (!Character.isJavaIdentifierPart(cp = path.codePointAt(i)))) {
                break;
            }
            i += Character.charCount(cp);
        }
        return i;
    }

    private static String extractIdentifier(String path, int beginIndex, int endIndex) {
        return path.substring(beginIndex, endIndex);
    }

    private static int readCharacterString(String path, int tokenStart, int end, int i, char quote,
            ArrayList<PathToken> tokens) {
        StringBuilder builder = new StringBuilder();
        i++;
        while (i <= end) {
            int cp = path.codePointAt(i);
            i += Character.charCount(cp);
            if (cp == quote) {
                tokens.add(new PathToken.CharacterStringToken(tokenStart, builder.toString()));
                return i;
            }
            if (cp == '\\') {
                if (i > end) {
                    throw DbException.getSyntaxError(path, tokenStart);
                }
                char c = path.charAt(i++);
                switch (c) {
                case '"':
                case '\'':
                case '\\':
                    builder.append(c);
                    break;
                case '0':
                    builder.append((char) 0);
                    break;
                case '\n':
                    break;
                case '\r': {
                    if (i < end && path.charAt(i) == '\n') {
                        i++;
                    }
                    break;
                }
                case 'b':
                    builder.append('\b');
                    break;
                case 'f':
                    builder.append('\f');
                    break;
                case 'n':
                    builder.append('\n');
                    break;
                case 'r':
                    builder.append('\r');
                    break;
                case 't':
                    builder.append('\t');
                    break;
                case 'u':
                    if (i + 3 > end) {
                        throw DbException.getSyntaxError(path, tokenStart);
                    }
                    c = path.charAt(i);
                    if (c == '{') {
                        int idx = path.indexOf('}', i + 2);
                        if (idx < 0) {
                            throw DbException.getSyntaxError(path, tokenStart);
                        }
                        try {
                            cp = Integer.parseInt(path, i + 1, idx - 1, 16);
                        } catch (NumberFormatException e) {
                            throw DbException.getSyntaxError(path, tokenStart);
                        }
                        i = idx + 1;
                        builder.appendCodePoint(cp);
                    } else {
                        try {
                            c = (char) Integer.parseInt(path, i, i + 3, 16);
                        } catch (NumberFormatException e) {
                            throw DbException.getSyntaxError(path, tokenStart);
                        }
                        i += 2;
                        builder.append(c);
                    }
                    break;
                case 'v':
                    builder.append((char) 0x000B);
                    break;
                case 'x':
                    if (i + 1 > end) {
                        throw DbException.getSyntaxError(path, tokenStart);
                    }
                    try {
                        c = (char) Integer.parseInt(path, i, i + 1, 16);
                    } catch (NumberFormatException e) {
                        throw DbException.getSyntaxError(path, tokenStart);
                    }
                    i += 2;
                    builder.append(c);
                    break;
                default:
                    throw DbException.getSyntaxError(path, tokenStart);
                }
            } else {
                builder.appendCodePoint(cp);
            }
        }
        throw DbException.getSyntaxError(path, tokenStart);
    }

    private static int readIntegerNumber(String path, int tokenStart, int end, int i, ArrayList<PathToken> tokens,
            String name, int radix) {
        if (i > end) {
            throw DbException.getSyntaxError(path, tokenStart, name);
        }
        int maxDigit, maxLetter;
        if (radix > 10) {
            maxDigit = '9';
            maxLetter = ('A' - 11) + radix;
        } else {
            maxDigit = ('0' - 1) + radix;
            maxLetter = -1;
        }
        int start = i;
        long number = 0;
        char c;
        int lastUnderscore = Integer.MIN_VALUE;
        do {
            c = path.charAt(i);
            if (c >= '0' && c <= maxDigit) {
                number = (number * radix) + c - '0';
            } else if (c == '_') {
                if (lastUnderscore == i - 1) {
                    throw DbException.getSyntaxError(path, tokenStart, name);
                }
                lastUnderscore = i;
                continue;
            } else if (maxLetter >= 0 && (c &= 0xffdf) >= 'A' && c <= maxLetter) {
                number = (number * radix) + c - ('A' - 10);
            } else if (i == start) {
                throw DbException.getSyntaxError(path, tokenStart, name);
            } else {
                break;
            }
            if (number > Integer.MAX_VALUE) {
                while (++i <= end) {
                    if ((c = path.charAt(i)) >= '0' && c <= maxDigit) {
                        //
                    } else if (c == '_') {
                        if (lastUnderscore == i - 1) {
                            throw DbException.getSyntaxError(path, tokenStart, name);
                        }
                        lastUnderscore = i;
                        continue;
                    } else if (maxLetter >= 0 && (c &= 0xffdf) >= 'A' && c <= 'F') {
                        //
                    } else {
                        break;
                    }
                }
                if (lastUnderscore == i - 1) {
                    throw DbException.getSyntaxError(path, tokenStart, name);
                }
                return finishBigInteger(path, tokenStart, end, i, start, lastUnderscore >= 0, radix, tokens);
            }
        } while (++i <= end);
        if ((lastUnderscore == i - 1) || (i <= end && Character.isJavaIdentifierPart(path.codePointAt(i)))) {
            throw DbException.getSyntaxError(path, tokenStart, name);
        }
        tokens.add(new PathToken.IntegerToken(start, (int) number));
        return i;
    }

    private static int readNumeric(String path, int tokenStart, int end, int i, char c, ArrayList<PathToken> tokens) {
        long number = c - '0';
        int lastUnderscore = Integer.MIN_VALUE;
        for (; i <= end; i++) {
            c = path.charAt(i);
            if (c < '0' || c > '9') {
                if (lastUnderscore == i - 1) {
                    throw DbException.getSyntaxError(path, tokenStart, "Numeric");
                }
                switch (c) {
                case '.':
                    return readFloat(path, tokenStart, end, i, lastUnderscore >= 0, tokens);
                case 'E':
                case 'e':
                    return readApproximateNumeric(path, tokenStart, end, i, lastUnderscore >= 0, tokens);
                case '_':
                    lastUnderscore = i;
                    continue;
                }
                break;
            }
            number = number * 10 + (c - '0');
            if (number > Integer.MAX_VALUE) {
                while (++i <= end) {
                    c = path.charAt(i);
                    if (c < '0' || c > '9') {
                        if (lastUnderscore == i - 1) {
                            throw DbException.getSyntaxError(path, tokenStart, "Numeric");
                        }
                        switch (c) {
                        case '.':
                            return readFloat(path, tokenStart, end, i, lastUnderscore >= 0, tokens);
                        case 'E':
                        case 'e':
                            return readApproximateNumeric(path, tokenStart, end, i, lastUnderscore >= 0, tokens);
                        case '_':
                            lastUnderscore = i;
                            continue;
                        }
                        break;
                    }
                }
                if (lastUnderscore == i - 1) {
                    throw DbException.getSyntaxError(path, tokenStart, "Numeric");
                }
                return finishBigInteger(path, tokenStart, end, i, tokenStart, lastUnderscore >= 0, 10, tokens);
            }
        }
        if (lastUnderscore == i - 1) {
            throw DbException.getSyntaxError(path, tokenStart, "Numeric");
        }
        tokens.add(new PathToken.IntegerToken(tokenStart, (int) number));
        return i;
    }

    private static int readFloat(String path, int tokenStart, int end, int i, boolean withUnderscore,
            ArrayList<PathToken> tokens) {
        int start = i + 1;
        int lastUnderscore = Integer.MIN_VALUE;
        while (++i <= end) {
            char c = path.charAt(i);
            if (c < '0' || c > '9') {
                if (lastUnderscore == i - 1) {
                    throw DbException.getSyntaxError(path, tokenStart, "Numeric");
                }
                switch (c) {
                case 'E':
                case 'e':
                    return readApproximateNumeric(path, tokenStart, end, i, withUnderscore, tokens);
                case '_':
                    if (i == start) {
                        throw DbException.getSyntaxError(path, tokenStart, "Numeric");
                    }
                    lastUnderscore = i;
                    withUnderscore = true;
                    continue;
                }
                break;
            }
        }
        if (lastUnderscore == i - 1) {
            throw DbException.getSyntaxError(path, tokenStart, "Numeric");
        }
        tokens.add(new PathToken.BigDecimalToken(tokenStart, readBigDecimal(path, tokenStart, i, withUnderscore)));
        return i;
    }

    private static int readApproximateNumeric(String path, int tokenStart, int end, int i, boolean withUnderscore,
            ArrayList<PathToken> tokens) {
        if (i == end) {
            throw DbException.getSyntaxError(path, tokenStart, "Approximate numeric");
        }
        char c = path.charAt(++i);
        if (c == '+' || c == '-') {
            if (i == end) {
                throw DbException.getSyntaxError(path, tokenStart, "Approximate numeric");
            }
            c = path.charAt(++i);
        }
        if (c < '0' || c > '9') {
            throw DbException.getSyntaxError(path, tokenStart, "Approximate numeric");
        }
        int lastUnderscore = Integer.MIN_VALUE;
        while (++i <= end) {
            c = path.charAt(i);
            if (c < '0' || c > '9') {
                if (lastUnderscore == i - 1) {
                    throw DbException.getSyntaxError(path, tokenStart, "Approximate numeric");
                }
                if (c == '_') {
                    lastUnderscore = i;
                    withUnderscore = true;
                    continue;
                }
                break;
            }
        }
        if (lastUnderscore == i - 1) {
            throw DbException.getSyntaxError(path, tokenStart, "Approximate numeric");
        }
        tokens.add(new PathToken.BigDecimalToken(tokenStart, readBigDecimal(path, tokenStart, i, withUnderscore)));
        return i;
    }

    private static BigDecimal readBigDecimal(String path, int tokenStart, int i, boolean withUnderscore) {
        String string = readAndRemoveUnderscores(path, tokenStart, i, withUnderscore);
        BigDecimal bd;
        try {
            bd = new BigDecimal(string);
        } catch (NumberFormatException e) {
            throw DbException.getSyntaxError(path, tokenStart, "Numeric");
        }
        return bd;
    }

    private static int finishBigInteger(String path, int tokenStart, int end, int i, int start, boolean withUnderscore,
            int radix, ArrayList<PathToken> tokens) {
        int endIndex = i;
        if (radix == 16 && i <= end && Character.isJavaIdentifierPart(path.codePointAt(i))) {
            throw DbException.getSyntaxError(path, tokenStart, "Hex number");
        }
        tokens.add(new PathToken.BigDecimalToken(tokenStart, new BigDecimal(
                new BigInteger(readAndRemoveUnderscores(path, start, endIndex, withUnderscore), radix))));
        return i;
    }

    private static String readAndRemoveUnderscores(String path, int start, int endIndex, boolean withUnderscore) {
        if (!withUnderscore) {
            return path.substring(start, endIndex);
        }
        StringBuilder builder = new StringBuilder(endIndex - start - 1);
        for (; start < endIndex; start++) {
            char c = path.charAt(start);
            if (c != '_') {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    private PathTokenizer() {
    }

}
