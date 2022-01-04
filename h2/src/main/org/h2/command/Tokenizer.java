/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import static org.h2.command.Token.ASTERISK;
import static org.h2.command.Token.AT;
import static org.h2.command.Token.BIGGER;
import static org.h2.command.Token.BIGGER_EQUAL;
import static org.h2.command.Token.CLOSE_BRACE;
import static org.h2.command.Token.CLOSE_BRACKET;
import static org.h2.command.Token.CLOSE_PAREN;
import static org.h2.command.Token.COLON;
import static org.h2.command.Token.COLON_COLON;
import static org.h2.command.Token.COLON_EQ;
import static org.h2.command.Token.COMMA;
import static org.h2.command.Token.CONCATENATION;
import static org.h2.command.Token.DOT;
import static org.h2.command.Token.EQUAL;
import static org.h2.command.Token.MINUS_SIGN;
import static org.h2.command.Token.NOT_EQUAL;
import static org.h2.command.Token.NOT_TILDE;
import static org.h2.command.Token.OPEN_BRACE;
import static org.h2.command.Token.OPEN_BRACKET;
import static org.h2.command.Token.OPEN_PAREN;
import static org.h2.command.Token.PERCENT;
import static org.h2.command.Token.PLUS_SIGN;
import static org.h2.command.Token.SEMICOLON;
import static org.h2.command.Token.SLASH;
import static org.h2.command.Token.SMALLER;
import static org.h2.command.Token.SMALLER_EQUAL;
import static org.h2.command.Token.SPATIAL_INTERSECTS;
import static org.h2.command.Token.TILDE;
import static org.h2.util.ParserUtil.IDENTIFIER;
import static org.h2.util.ParserUtil.LIMIT;
import static org.h2.util.ParserUtil.MINUS;
import static org.h2.util.ParserUtil.UESCAPE;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.ListIterator;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueNumeric;

/**
 * Tokenizer.
 */
public final class Tokenizer {

    private Tokenizer() {
    }

    static ArrayList<Token> tokenize(String sql, CastDataProvider provider, boolean identifiersToUpper,
            boolean identifiersToLower, BitSet nonKeywords, boolean stopOnCloseParen) {
        ArrayList<Token> tokens = new ArrayList<>();
        int end = sql.length() - 1;
        boolean foundUnicode = false;
        loop: for (int i = 0; i <= end;) {
            int tokenStart = i;
            char c = sql.charAt(i);
            Token token;
            switch (c) {
            case '!':
                if (i < end) {
                    char c2 = sql.charAt(++i);
                    if (c2 == '=') {
                        token = new Token.KeywordToken(tokenStart, NOT_EQUAL);
                        break;
                    }
                    if (c2 == '~') {
                        token = new Token.KeywordToken(tokenStart, NOT_TILDE);
                        break;
                    }
                }
                throw DbException.getSyntaxError(sql, tokenStart);
            case '"':
            case '`':
                i = readQuotedIdentifier(sql, end, identifiersToUpper, identifiersToLower, tokenStart, i, c, false,
                        tokens);
                continue loop;
            case '#':
                if (provider.getMode().supportPoundSymbolForColumnNames) {
                    i = readIdentifier(sql, provider, end, identifiersToUpper, identifiersToLower, tokenStart, i, c,
                            tokens);
                    continue loop;
                }
                throw DbException.getSyntaxError(sql, tokenStart);
            case '$':
                if (i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 == '$') {
                        i += 2;
                        int stringEnd = sql.indexOf("$$", i);
                        if (stringEnd < 0) {
                            throw DbException.getSyntaxError(sql, tokenStart);
                        }
                        token = new Token.CharacterStringToken(tokenStart, sql.substring(i, stringEnd), false);
                        i = stringEnd + 1;
                    } else {
                        i = parseParameterIndex(sql, end, i, tokens);
                        continue loop;
                    }
                } else {
                    token = new Token.ParameterToken(tokenStart, 0);
                }
                break;
            case '%':
                token = new Token.KeywordToken(tokenStart, PERCENT);
                break;
            case '&':
                if (i < end && sql.charAt(i + 1) == '&') {
                    i++;
                    token = new Token.KeywordToken(tokenStart, SPATIAL_INTERSECTS);
                    break;
                }
                throw DbException.getSyntaxError(sql, tokenStart);
            case '\'':
                i = readCharacterString(sql, provider, tokenStart, end, i, false, tokens);
                continue loop;
            case '(':
                token = new Token.KeywordToken(tokenStart, OPEN_PAREN);
                break;
            case ')':
                token = new Token.KeywordToken(tokenStart, CLOSE_PAREN);
                if (stopOnCloseParen) {
                    tokens.add(token);
                    end = skipWhitespace(sql, end, i + 1) - 1;
                    break loop;
                }
                break;
            case '*':
                token = new Token.KeywordToken(tokenStart, ASTERISK);
                break;
            case '+':
                token = new Token.KeywordToken(tokenStart, PLUS_SIGN);
                break;
            case ',':
                token = new Token.KeywordToken(tokenStart, COMMA);
                break;
            case '-':
                if (i < end && sql.charAt(i + 1) == '-') {
                    i = skipSimpleComment(sql, end, i);
                    continue loop;
                } else {
                    token = new Token.KeywordToken(tokenStart, MINUS_SIGN);
                }
                break;
            case '.':
                if (i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 >= '0' && c2 <= '9') {
                        i = readNumeric(sql, tokenStart, end, i + 1, c2, false, false, tokens);
                        continue loop;
                    }
                }
                token = new Token.KeywordToken(tokenStart, DOT);
                break;
            case '/':
                if (i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 == '*') {
                        i = skipBracketedComment(sql, tokenStart, end, i);
                        continue loop;
                    } else if (c2 == '/') {
                        i = skipSimpleComment(sql, end, i);
                        continue loop;
                    }
                }
                token = new Token.KeywordToken(tokenStart, SLASH);
                break;
            case '0':
                if (i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 == 'X' || c2 == 'x') {
                        i = readHexNumber(sql, provider, tokenStart, end, i + 2, tokens);
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
                i = readNumeric(sql, tokenStart, end, i + 1, c, tokens);
                continue loop;
            case ':':
                if (i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 == ':') {
                        i++;
                        token = new Token.KeywordToken(tokenStart, COLON_COLON);
                        break;
                    } else if (c2 == '=') {
                        i++;
                        token = new Token.KeywordToken(tokenStart, COLON_EQ);
                        break;
                    }
                }
                token = new Token.KeywordToken(tokenStart, COLON);
                break;
            case ';':
                token = new Token.KeywordToken(tokenStart, SEMICOLON);
                break;
            case '<':
                if (i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 == '=') {
                        i++;
                        token = new Token.KeywordToken(tokenStart, SMALLER_EQUAL);
                        break;
                    }
                    if (c2 == '>') {
                        i++;
                        token = new Token.KeywordToken(tokenStart, NOT_EQUAL);
                        break;
                    }
                }
                token = new Token.KeywordToken(tokenStart, SMALLER);
                break;
            case '=':
                token = new Token.KeywordToken(tokenStart, EQUAL);
                break;
            case '>':
                if (i < end && sql.charAt(i + 1) == '=') {
                    i++;
                    token = new Token.KeywordToken(tokenStart, BIGGER_EQUAL);
                    break;
                }
                token = new Token.KeywordToken(tokenStart, BIGGER);
                break;
            case '?':
                if (i + 1 < end && sql.charAt(i + 1) == '?') {
                    char c3 = sql.charAt(i + 2);
                    if (c3 == '(') {
                        i += 2;
                        token = new Token.KeywordToken(tokenStart, OPEN_BRACKET);
                        break;
                    }
                    if (c3 == ')') {
                        i += 2;
                        token = new Token.KeywordToken(tokenStart, CLOSE_BRACKET);
                        break;
                    }
                }
                i = parseParameterIndex(sql, end, i, tokens);
                continue loop;
            case '@':
                token = new Token.KeywordToken(tokenStart, AT);
                break;
            case 'A':
            case 'B':
            case 'C':
            case 'D':
            case 'E':
            case 'F':
            case 'G':
            case 'H':
            case 'I':
            case 'J':
            case 'K':
            case 'L':
            case 'M':
            case 'O':
            case 'P':
            case 'Q':
            case 'R':
            case 'S':
            case 'T':
            case 'V':
            case 'W':
            case 'Y':
            case 'Z':
            case '_':
            case 'a':
            case 'b':
            case 'c':
            case 'd':
            case 'e':
            case 'f':
            case 'g':
            case 'h':
            case 'i':
            case 'j':
            case 'k':
            case 'l':
            case 'm':
            case 'o':
            case 'p':
            case 'q':
            case 'r':
            case 's':
            case 't':
            case 'v':
            case 'w':
            case 'y':
            case 'z':
                i = readIdentifierOrKeyword(sql, provider, end, identifiersToUpper, identifiersToLower, nonKeywords,
                        tokenStart, i, c, tokens);
                continue loop;
            case 'N':
            case 'n':
                if (i < end && sql.charAt(i + 1) == '\'') {
                    i = readCharacterString(sql, provider, tokenStart, end, i + 1, false, tokens);
                    continue loop;
                }
                i = readIdentifierOrKeyword(sql, provider, end, identifiersToUpper, identifiersToLower, nonKeywords,
                        tokenStart, i, c, tokens);
                continue loop;
            case 'X':
            case 'x':
                if (i < end && sql.charAt(i + 1) == '\'') {
                    i = readBinaryString(sql, provider, tokenStart, end, i + 1, tokens);
                    continue loop;
                }
                i = readIdentifierOrKeyword(sql, provider, end, identifiersToUpper, identifiersToLower, nonKeywords,
                        tokenStart, i, c, tokens);
                continue loop;
            case 'U':
            case 'u':
                if (i + 1 < end && sql.charAt(i + 1) == '&') {
                    char c3 = sql.charAt(i + 2);
                    if (c3 == '"') {
                        i = readQuotedIdentifier(sql, end, identifiersToUpper, identifiersToLower, tokenStart, //
                                i + 2, '"', true, tokens);
                        foundUnicode = true;
                        continue loop;
                    } else if (c3 == '\'') {
                        i = readCharacterString(sql, provider, tokenStart, end, i + 2, true, tokens);
                        foundUnicode = true;
                        continue loop;
                    }
                }
                i = readIdentifierOrKeyword(sql, provider, end, identifiersToUpper, identifiersToLower, nonKeywords,
                        tokenStart, i, c, tokens);
                continue loop;
            case '[':
                if (provider.getMode().squareBracketQuotedNames) {
                    int identifierEnd = sql.indexOf(']', ++i);
                    if (identifierEnd < 0) {
                        throw DbException.getSyntaxError(sql, tokenStart);
                    }
                    token = new Token.IdentifierToken(tokenStart, sql.substring(i, identifierEnd), true, false);
                    i = identifierEnd;
                } else {
                    token = new Token.KeywordToken(tokenStart, OPEN_BRACKET);
                }
                break;
            case ']':
                token = new Token.KeywordToken(tokenStart, CLOSE_BRACKET);
                break;
            case '{':
                token = new Token.KeywordToken(tokenStart, OPEN_BRACE);
                break;
            case '|':
                if (i < end && sql.charAt(++i) == '|') {
                    token = new Token.KeywordToken(tokenStart, CONCATENATION);
                    break;
                }
                throw DbException.getSyntaxError(sql, tokenStart);
            case '}':
                token = new Token.KeywordToken(tokenStart, CLOSE_BRACE);
                break;
            case '~':
                token = new Token.KeywordToken(tokenStart, TILDE);
                break;
            default:
                if (c <= ' ') {
                    i++;
                    continue loop;
                } else {
                    int cp = Character.isHighSurrogate(c) ? sql.codePointAt(i++) : c;
                    if (Character.isSpaceChar(cp)) {
                        continue loop;
                    }
                    if (Character.isJavaIdentifierStart(cp)) {
                        i = readIdentifier(sql, provider, end, identifiersToUpper, identifiersToLower, tokenStart, i,
                                cp, tokens);
                        continue loop;
                    }
                    throw DbException.getSyntaxError(sql, tokenStart);
                }
            }
            tokens.add(token);
            i++;
        }
        if (foundUnicode) {
            processUescape(sql, tokens);
        }
        tokens.add(new Token.EndOfInputToken(end + 1));
        return tokens;
    }

    private static int readIdentifier(String sql, CastDataProvider provider, int end, boolean identifiersToUpper, //
            boolean identifiersToLower, int tokenStart, int i, int cp, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, provider, end, i, cp);
        tokens.add(new Token.IdentifierToken(tokenStart,
                extractIdentifier(sql, identifiersToUpper, identifiersToLower, tokenStart, endIndex), false, false));
        return endIndex;
    }

    private static int readIdentifierOrKeyword(String sql, CastDataProvider provider, int end,
            boolean identifiersToUpper, boolean identifiersToLower, BitSet nonKeywords, int tokenStart, int i, char c,
            ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, provider, end, i, c);
        int type = ParserUtil.getTokenType(sql, true, tokenStart, endIndex - tokenStart, false);
        Token token;
        boolean keyword;
        switch (type) {
        case IDENTIFIER:
            keyword = false;
            break;
        case LIMIT:
            keyword = provider.getMode().limit;
            break;
        case MINUS:
            keyword = provider.getMode().minusIsExcept;
            break;
        default:
            keyword = true;
        }
        if (!keyword) {
            token = new Token.IdentifierToken(tokenStart,
                    extractIdentifier(sql, identifiersToUpper, identifiersToLower, tokenStart, endIndex), //
                    false, false);
        } else if (nonKeywords != null && nonKeywords.get(type)) {
            token = new Token.KeywordOrIdentifierToken(tokenStart, type,
                    extractIdentifier(sql, identifiersToUpper, identifiersToLower, tokenStart, endIndex));
        } else {
            token = new Token.KeywordToken(tokenStart, type);
        }
        tokens.add(token);
        return endIndex;
    }

    private static int findIdentifierEnd(String sql, CastDataProvider provider, int end, int i, int cp) {
        int next = i;
        for (;; i = next) {
            next += Character.charCount(cp);
            if (next > end || (!Character.isJavaIdentifierPart(cp = sql.codePointAt(next))
                    && (cp != '#' || !provider.getMode().supportPoundSymbolForColumnNames))) {
                break;
            }
        }
        return next;
    }

    private static String extractIdentifier(String sql, boolean identifiersToUpper, boolean identifiersToLower,
            int beginIndex, int endIndex) {
        return convertCase(identifiersToUpper, identifiersToLower, sql.substring(beginIndex, endIndex));
    }

    private static int readQuotedIdentifier(String sql, int end, boolean identifiersToUpper, //
            boolean identifiersToLower, int tokenStart, int i, char c, boolean unicode, ArrayList<Token> tokens) {
        int identifierEnd = sql.indexOf(c, ++i);
        if (identifierEnd < 0) {
            throw DbException.getSyntaxError(sql, tokenStart);
        }
        String s = sql.substring(i, identifierEnd);
        i = identifierEnd + 1;
        if (i <= end && sql.charAt(i) == c) {
            StringBuilder builder = new StringBuilder(s);
            do {
                identifierEnd = sql.indexOf(c, i + 1);
                if (identifierEnd < 0) {
                    throw DbException.getSyntaxError(sql, tokenStart);
                }
                builder.append(sql, i, identifierEnd);
                i = identifierEnd + 1;
            } while (i <= end && sql.charAt(i) == c);
            s = builder.toString();
        }
        if (c == '`') {
            s = convertCase(identifiersToUpper, identifiersToLower, s);
        }
        tokens.add(new Token.IdentifierToken(tokenStart, s, true, unicode));
        return i;
    }

    private static String convertCase(boolean identifiersToUpper, boolean identifiersToLower, String s) {
        if (identifiersToUpper) {
            s = StringUtils.toUpperEnglish(s);
        } else if (identifiersToLower) {
            s = StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    private static int readBinaryString(String sql, CastDataProvider provider, int tokenStart, int end, int i,
            ArrayList<Token> tokens) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        int stringEnd;
        do {
            stringEnd = sql.indexOf('\'', ++i);
            if (stringEnd < 0 || stringEnd < end && sql.charAt(stringEnd + 1) == '\'') {
                throw DbException.getSyntaxError(sql, tokenStart);
            }
            StringUtils.convertHexWithSpacesToBytes(result, sql, i, stringEnd);
            i = skipWhitespace(sql, end, stringEnd + 1);
        } while (i <= end && sql.charAt(i) == '\'');
        tokens.add(new Token.BinaryStringToken(tokenStart, result.toByteArray()));
        return i;
    }

    private static int readCharacterString(String sql, CastDataProvider provider, int tokenStart, int end, int i,
            boolean unicode, ArrayList<Token> tokens) {
        String s = null;
        StringBuilder builder = null;
        int stringEnd;
        do {
            stringEnd = sql.indexOf('\'', ++i);
            if (stringEnd < 0) {
                throw DbException.getSyntaxError(sql, tokenStart);
            }
            if (s == null) {
                s = sql.substring(i, stringEnd);
            } else {
                if (builder == null) {
                    builder = new StringBuilder(s);
                }
                builder.append(sql, i, stringEnd);
            }
            i = stringEnd + 1;
            if (i <= end && sql.charAt(i) == '\'') {
                if (builder == null) {
                    builder = new StringBuilder(s);
                }
                do {
                    stringEnd = sql.indexOf('\'', i + 1);
                    if (stringEnd < 0) {
                        throw DbException.getSyntaxError(sql, tokenStart);
                    }
                    builder.append(sql, i, stringEnd);
                    i = stringEnd + 1;
                } while (i <= end && sql.charAt(i) == '\'');
            }
            i = skipWhitespace(sql, end, i);
        } while (i <= end && sql.charAt(i) == '\'');
        if (builder != null) {
            s = builder.toString();
        }
        tokens.add(new Token.CharacterStringToken(tokenStart, s, unicode));
        return i;
    }

    private static int skipWhitespace(String sql, int end, int i) {
        while (i <= end) {
            int cp = sql.codePointAt(i);
            if (!Character.isWhitespace(cp)) {
                if (cp == '/' && i < end) {
                    char c2 = sql.charAt(i + 1);
                    if (c2 == '*') {
                        i = skipBracketedComment(sql, i, end, i);
                        continue;
                    } else if (c2 == '/') {
                        i = skipSimpleComment(sql, end, i);
                        continue;
                    }
                }
                break;
            }
            i += Character.charCount(cp);
        }
        return i;
    }

    private static int readHexNumber(String sql, CastDataProvider provider, int tokenStart, int end, int i,
            ArrayList<Token> tokens) {
        if (provider.getMode().zeroExLiteralsAreBinaryStrings) {
            int start = i;
            for (char c; i <= end
                    && (((c = sql.charAt(i)) >= '0' && c <= '9') || ((c &= 0xffdf) >= 'A' && c <= 'F'));) {
                i++;
            }
            if (i <= end && Character.isJavaIdentifierPart(sql.codePointAt(i))) {
                throw DbException.get(ErrorCode.HEX_STRING_WRONG_1, sql.substring(start, i + 1));
            }
            tokens.add(new Token.BinaryStringToken(start, StringUtils.convertHexToBytes(sql.substring(start, i))));
            return i;
        } else {
            if (i > end) {
                throw DbException.getSyntaxError(sql, tokenStart, "Hex number");
            }
            int start = i;
            long number = 0;
            char c;
            do {
                c = sql.charAt(i);
                if (c >= '0' && c <= '9') {
                    number = (number << 4) + c - '0';
                    // Convert a-z to A-Z
                } else if ((c &= 0xffdf) >= 'A' && c <= 'F') {
                    number = (number << 4) + c - ('A' - 10);
                } else if (i == start) {
                    throw DbException.getSyntaxError(sql, tokenStart, "Hex number");
                } else {
                    break;
                }
                if (number > Integer.MAX_VALUE) {
                    while (++i <= end
                            && (((c = sql.charAt(i)) >= '0' && c <= '9') || ((c &= 0xffdf) >= 'A' && c <= 'F'))) {
                    }
                    return finishBigInteger(sql, tokenStart, end, i, start, i <= end && c == 'L', 16, tokens);
                }
            } while (++i <= end);

            boolean bigint = i <= end && c == 'L';
            if (bigint) {
                i++;
            }
            if (i <= end && Character.isJavaIdentifierPart(sql.codePointAt(i))) {
                throw DbException.getSyntaxError(sql, tokenStart, "Hex number");
            }
            tokens.add(bigint ? new Token.BigintToken(start, number) : new Token.IntegerToken(start, (int) number));
            return i;
        }
    }

    private static int readNumeric(String sql, int tokenStart, int end, int i, char c, ArrayList<Token> tokens) {
        long number = c - '0';
        for (; i <= end; i++) {
            c = sql.charAt(i);
            if (c < '0' || c > '9') {
                switch (c) {
                case '.':
                    return readNumeric(sql, tokenStart, end, i, c, false, false, tokens);
                case 'E':
                case 'e':
                    return readNumeric(sql, tokenStart, end, i, c, false, true, tokens);
                case 'L':
                case 'l':
                    return finishBigInteger(sql, tokenStart, end, i, tokenStart, true, 10, tokens);
                }
                break;
            }
            number = number * 10 + (c - '0');
            if (number > Integer.MAX_VALUE) {
                return readNumeric(sql, tokenStart, end, i, c, true, false, tokens);
            }
        }
        tokens.add(new Token.IntegerToken(tokenStart, (int) number));
        return i;
    }

    private static int readNumeric(String sql, int tokenStart, int end, int i, char c, boolean integer,
            boolean approximate, ArrayList<Token> tokens) {
        if (!approximate) {
            while (++i <= end) {
                c = sql.charAt(i);
                if (c == '.') {
                    integer = false;
                } else if (c < '0' || c > '9') {
                    break;
                }
            }
        }
        if (i <= end && (c == 'E' || c == 'e')) {
            integer = false;
            approximate = true;
            if (i == end) {
                throw DbException.getSyntaxError(sql, tokenStart);
            }
            c = sql.charAt(++i);
            if (c == '+' || c == '-') {
                if (i == end) {
                    throw DbException.getSyntaxError(sql, tokenStart);
                }
                c = sql.charAt(++i);
            }
            if (c < '0' || c > '9') {
                throw DbException.getSyntaxError(sql, tokenStart);
            }
            while (++i <= end && (c = sql.charAt(i)) >= '0' && c <= '9') {
                // go until the first non-number
            }
        }
        if (integer) {
            return finishBigInteger(sql, tokenStart, end, i, tokenStart, i < end && c == 'L' || c == 'l', 10, tokens);
        }
        BigDecimal bd;
        String string = sql.substring(tokenStart, i);
        try {
            bd = new BigDecimal(string);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, string);
        }
        tokens.add(new Token.ValueToken(tokenStart, approximate ? ValueDecfloat.get(bd) : ValueNumeric.get(bd)));
        return i;
    }

    private static int finishBigInteger(String sql, int tokenStart, int end, int i, int start, boolean asBigint,
            int radix, ArrayList<Token> tokens) {
        int endIndex = i;
        if (asBigint) {
            i++;
        }
        if (radix == 16 && i <= end && Character.isJavaIdentifierPart(sql.codePointAt(i))) {
            throw DbException.getSyntaxError(sql, tokenStart, "Hex number");
        }
        BigInteger bigInteger = new BigInteger(sql.substring(start, endIndex), radix);
        Token token;
        if (bigInteger.compareTo(ValueBigint.MAX_BI) > 0) {
            if (asBigint) {
                throw DbException.getSyntaxError(sql, tokenStart);
            }
            token = new Token.ValueToken(tokenStart, ValueNumeric.get(bigInteger));
        } else {
            token = new Token.BigintToken(start, bigInteger.longValue());
        }
        tokens.add(token);
        return i;
    }

    private static int skipBracketedComment(String sql, int tokenStart, int end, int i) {
        i += 2;
        for (int level = 1; level > 0;) {
            for (;;) {
                if (i >= end) {
                    throw DbException.getSyntaxError(sql, tokenStart);
                }
                char c = sql.charAt(i++);
                if (c == '*') {
                    if (sql.charAt(i) == '/') {
                        level--;
                        i++;
                        break;
                    }
                } else if (c == '/' && sql.charAt(i) == '*') {
                    level++;
                    i++;
                }
            }
        }
        return i;
    }

    private static int skipSimpleComment(String sql, int end, int i) {
        i += 2;
        for (char c; i <= end && (c = sql.charAt(i)) != '\n' && c != '\r'; i++) {
            //
        }
        return i;
    }

    private static int parseParameterIndex(String sql, int end, int i, ArrayList<Token> tokens) {
        int tokenStart = i;
        long number = 0;
        for (char c; ++i <= end && (c = sql.charAt(i)) >= '0' && c <= '9';) {
            number = number * 10 + (c - '0');
            if (number > Integer.MAX_VALUE) {
                throw DbException.getInvalidValueException("parameter index", number);
            }
        }
        if (i > tokenStart + 1 && number == 0) {
            throw DbException.getInvalidValueException("parameter index", number);
        }
        tokens.add(new Token.ParameterToken(tokenStart, (int) number));
        return i;
    }

    private static void processUescape(String sql, ArrayList<Token> tokens) {
        ListIterator<Token> i = tokens.listIterator();
        while (i.hasNext()) {
            Token token = i.next();
            if (token.needsUnicodeConversion()) {
                int uescape = '\\';
                condition: if (i.hasNext()) {
                    Token t2 = i.next();
                    if (t2.tokenType() == UESCAPE) {
                        i.remove();
                        if (i.hasNext()) {
                            Token t3 = i.next();
                            i.remove();
                            if (t3 instanceof Token.CharacterStringToken) {
                                String s = ((Token.CharacterStringToken) t3).string;
                                if (s.codePointCount(0, s.length()) == 1) {
                                    int escape = s.codePointAt(0);
                                    if (!Character.isWhitespace(escape) && (escape < '0' || escape > '9')
                                            && (escape < 'A' || escape > 'F') && (escape < 'a' || escape > 'f')) {
                                        switch (escape) {
                                        default:
                                            uescape = escape;
                                            break condition;
                                        case '"':
                                        case '\'':
                                        case '+':
                                        }
                                    }
                                }
                            }
                        }
                        throw DbException.getSyntaxError(sql, t2.start() + 7, "'<Unicode escape character>'");
                    }
                }
                token.convertUnicode(uescape);
            }
        }
    }

}
