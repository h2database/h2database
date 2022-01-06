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
import static org.h2.util.ParserUtil.ALL;
import static org.h2.util.ParserUtil.AND;
import static org.h2.util.ParserUtil.ANY;
import static org.h2.util.ParserUtil.ARRAY;
import static org.h2.util.ParserUtil.AS;
import static org.h2.util.ParserUtil.ASYMMETRIC;
import static org.h2.util.ParserUtil.AUTHORIZATION;
import static org.h2.util.ParserUtil.BETWEEN;
import static org.h2.util.ParserUtil.CASE;
import static org.h2.util.ParserUtil.CAST;
import static org.h2.util.ParserUtil.CHECK;
import static org.h2.util.ParserUtil.CONSTRAINT;
import static org.h2.util.ParserUtil.CROSS;
import static org.h2.util.ParserUtil.CURRENT_CATALOG;
import static org.h2.util.ParserUtil.CURRENT_DATE;
import static org.h2.util.ParserUtil.CURRENT_PATH;
import static org.h2.util.ParserUtil.CURRENT_ROLE;
import static org.h2.util.ParserUtil.CURRENT_SCHEMA;
import static org.h2.util.ParserUtil.CURRENT_TIME;
import static org.h2.util.ParserUtil.CURRENT_TIMESTAMP;
import static org.h2.util.ParserUtil.CURRENT_USER;
import static org.h2.util.ParserUtil.DAY;
import static org.h2.util.ParserUtil.DEFAULT;
import static org.h2.util.ParserUtil.DISTINCT;
import static org.h2.util.ParserUtil.ELSE;
import static org.h2.util.ParserUtil.END;
import static org.h2.util.ParserUtil.EXCEPT;
import static org.h2.util.ParserUtil.EXISTS;
import static org.h2.util.ParserUtil.FALSE;
import static org.h2.util.ParserUtil.FETCH;
import static org.h2.util.ParserUtil.FOR;
import static org.h2.util.ParserUtil.FOREIGN;
import static org.h2.util.ParserUtil.FROM;
import static org.h2.util.ParserUtil.FULL;
import static org.h2.util.ParserUtil.GROUP;
import static org.h2.util.ParserUtil.HAVING;
import static org.h2.util.ParserUtil.HOUR;
import static org.h2.util.ParserUtil.IDENTIFIER;
import static org.h2.util.ParserUtil.IF;
import static org.h2.util.ParserUtil.IN;
import static org.h2.util.ParserUtil.INNER;
import static org.h2.util.ParserUtil.INTERSECT;
import static org.h2.util.ParserUtil.INTERVAL;
import static org.h2.util.ParserUtil.IS;
import static org.h2.util.ParserUtil.JOIN;
import static org.h2.util.ParserUtil.KEY;
import static org.h2.util.ParserUtil.LEFT;
import static org.h2.util.ParserUtil.LIKE;
import static org.h2.util.ParserUtil.LIMIT;
import static org.h2.util.ParserUtil.LOCALTIME;
import static org.h2.util.ParserUtil.LOCALTIMESTAMP;
import static org.h2.util.ParserUtil.MINUS;
import static org.h2.util.ParserUtil.MINUTE;
import static org.h2.util.ParserUtil.MONTH;
import static org.h2.util.ParserUtil.NATURAL;
import static org.h2.util.ParserUtil.NOT;
import static org.h2.util.ParserUtil.NULL;
import static org.h2.util.ParserUtil.OFFSET;
import static org.h2.util.ParserUtil.ON;
import static org.h2.util.ParserUtil.OR;
import static org.h2.util.ParserUtil.ORDER;
import static org.h2.util.ParserUtil.PRIMARY;
import static org.h2.util.ParserUtil.QUALIFY;
import static org.h2.util.ParserUtil.RIGHT;
import static org.h2.util.ParserUtil.ROW;
import static org.h2.util.ParserUtil.ROWNUM;
import static org.h2.util.ParserUtil.SECOND;
import static org.h2.util.ParserUtil.SELECT;
import static org.h2.util.ParserUtil.SESSION_USER;
import static org.h2.util.ParserUtil.SET;
import static org.h2.util.ParserUtil.SOME;
import static org.h2.util.ParserUtil.SYMMETRIC;
import static org.h2.util.ParserUtil.SYSTEM_USER;
import static org.h2.util.ParserUtil.TABLE;
import static org.h2.util.ParserUtil.TO;
import static org.h2.util.ParserUtil.TRUE;
import static org.h2.util.ParserUtil.UESCAPE;
import static org.h2.util.ParserUtil.UNION;
import static org.h2.util.ParserUtil.UNIQUE;
import static org.h2.util.ParserUtil.UNKNOWN;
import static org.h2.util.ParserUtil.USER;
import static org.h2.util.ParserUtil.USING;
import static org.h2.util.ParserUtil.VALUE;
import static org.h2.util.ParserUtil.VALUES;
import static org.h2.util.ParserUtil.WHEN;
import static org.h2.util.ParserUtil.WHERE;
import static org.h2.util.ParserUtil.WINDOW;
import static org.h2.util.ParserUtil.WITH;
import static org.h2.util.ParserUtil.YEAR;
import static org.h2.util.ParserUtil._ROWID_;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.ListIterator;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueNumeric;

/**
 * Tokenizer.
 */
public final class Tokenizer {

    private final CastDataProvider provider;

    private final boolean identifiersToUpper;

    private final boolean identifiersToLower;

    private final BitSet nonKeywords;

    Tokenizer(CastDataProvider provider, boolean identifiersToUpper, boolean identifiersToLower, BitSet nonKeywords) {
        this.provider = provider;
        this.identifiersToUpper = identifiersToUpper;
        this.identifiersToLower = identifiersToLower;
        this.nonKeywords = nonKeywords;
    }

    ArrayList<Token> tokenize(String sql, boolean stopOnCloseParen) {
        ArrayList<Token> tokens = new ArrayList<>();
        int end = sql.length() - 1;
        boolean foundUnicode = false;
        int lastParameter = 0;
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
                i = readQuotedIdentifier(sql, end, tokenStart, i, c, false, tokens);
                continue loop;
            case '#':
                if (provider.getMode().supportPoundSymbolForColumnNames) {
                    i = readIdentifier(sql, end, tokenStart, i, c, tokens);
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
                        lastParameter = assignParameterIndex(tokens, lastParameter);
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
                i = readCharacterString(sql, tokenStart, end, i, false, tokens);
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
            case '?': {
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
                lastParameter = assignParameterIndex(tokens, lastParameter);
                continue loop;
            }
            case '@':
                token = new Token.KeywordToken(tokenStart, AT);
                break;
            case 'A':
            case 'a':
                i = readA(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'B':
            case 'b':
                i = readB(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'C':
            case 'c':
                i = readC(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'D':
            case 'd':
                i = readD(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'E':
            case 'e':
                i = readE(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'F':
            case 'f':
                i = readF(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'G':
            case 'g':
                i = readG(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'H':
            case 'h':
                i = readH(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'I':
            case 'i':
                i = readI(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'J':
            case 'j':
                i = readJ(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'K':
            case 'k':
                i = readK(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'L':
            case 'l':
                i = readL(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'M':
            case 'm':
                i = readM(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'N':
            case 'n':
                if (i < end && sql.charAt(i + 1) == '\'') {
                    i = readCharacterString(sql, tokenStart, end, i + 1, false, tokens);
                } else {
                    i = readN(sql, end, tokenStart, i, tokens);
                }
                continue loop;
            case 'O':
            case 'o':
                i = readO(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'P':
            case 'p':
                i = readP(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'Q':
            case 'q':
                i = readQ(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'R':
            case 'r':
                i = readR(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'S':
            case 's':
                i = readS(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'T':
            case 't':
                i = readT(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'U':
            case 'u':
                if (i + 1 < end && sql.charAt(i + 1) == '&') {
                    char c3 = sql.charAt(i + 2);
                    if (c3 == '"') {
                        i = readQuotedIdentifier(sql, end, tokenStart, i + 2, '"', true, tokens);
                        foundUnicode = true;
                        continue loop;
                    } else if (c3 == '\'') {
                        i = readCharacterString(sql, tokenStart, end, i + 2, true, tokens);
                        foundUnicode = true;
                        continue loop;
                    }
                }
                i = readU(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'V':
            case 'v':
                i = readV(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'W':
            case 'w':
                i = readW(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'X':
            case 'x':
                if (i < end && sql.charAt(i + 1) == '\'') {
                    i = readBinaryString(sql, tokenStart, end, i + 1, tokens);
                } else {
                    i = readIdentifier(sql, end, tokenStart, i, c, tokens);
                }
                continue loop;
            case 'Y':
            case 'y':
                i = readY(sql, end, tokenStart, i, tokens);
                continue loop;
            case 'Z':
            case 'z':
                i = readIdentifier(sql, end, tokenStart, i, c, tokens);
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
            case '_':
                i = read_(sql, end, tokenStart, i, tokens);
                continue loop;
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
                        i = readIdentifier(sql, end, tokenStart, i, cp, tokens);
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

    private int readIdentifier(String sql, int end, int tokenStart, int i, int cp, ArrayList<Token> tokens) {
        if (cp >= Character.MIN_SUPPLEMENTARY_CODE_POINT) {
            i++;
        }
        int endIndex = findIdentifierEnd(sql, end, i + Character.charCount(cp) - 1);
        tokens.add(new Token.IdentifierToken(tokenStart, extractIdentifier(sql, tokenStart, endIndex), false, false));
        return endIndex;
    }

    private int readA(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (length == 2) {
            type = (sql.charAt(tokenStart + 1) & 0xffdf) == 'S' ? AS : IDENTIFIER;
        } else {
            if (eq("ALL", sql, tokenStart, length)) {
                type = ALL;
            } else if (eq("AND", sql, tokenStart, length)) {
                type = AND;
            } else if (eq("ANY", sql, tokenStart, length)) {
                type = ANY;
            } else if (eq("ARRAY", sql, tokenStart, length)) {
                type = ARRAY;
            } else if (eq("ASYMMETRIC", sql, tokenStart, length)) {
                type = ASYMMETRIC;
            } else if (eq("AUTHORIZATION", sql, tokenStart, length)) {
                type = AUTHORIZATION;
            } else {
                type = IDENTIFIER;
            }
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readB(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("BETWEEN", sql, tokenStart, length) ? BETWEEN : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readC(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("CASE", sql, tokenStart, length)) {
            type = CASE;
        } else if (eq("CAST", sql, tokenStart, length)) {
            type = CAST;
        } else if (eq("CHECK", sql, tokenStart, length)) {
            type = CHECK;
        } else if (eq("CONSTRAINT", sql, tokenStart, length)) {
            type = CONSTRAINT;
        } else if (eq("CROSS", sql, tokenStart, length)) {
            type = CROSS;
        } else if (length >= 12 && eq("CURRENT_", sql, tokenStart, 8)) {
            type = getTokenTypeCurrent(sql, tokenStart, length);
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private static int getTokenTypeCurrent(String s, int tokenStart, int length) {
        tokenStart += 8;
        switch (length) {
        case 12:
            if (eqCurrent("CURRENT_DATE", s, tokenStart, length)) {
                return CURRENT_DATE;
            } else if (eqCurrent("CURRENT_PATH", s, tokenStart, length)) {
                return CURRENT_PATH;
            } else if (eqCurrent("CURRENT_ROLE", s, tokenStart, length)) {
                return CURRENT_ROLE;
            } else if (eqCurrent("CURRENT_TIME", s, tokenStart, length)) {
                return CURRENT_TIME;
            } else if (eqCurrent("CURRENT_USER", s, tokenStart, length)) {
                return CURRENT_USER;
            }
            break;
        case 14:
            if (eqCurrent("CURRENT_SCHEMA", s, tokenStart, length)) {
                return CURRENT_SCHEMA;
            }
            break;
        case 15:
            if (eqCurrent("CURRENT_CATALOG", s, tokenStart, length)) {
                return CURRENT_CATALOG;
            }
            break;
        case 17:
            if (eqCurrent("CURRENT_TIMESTAMP", s, tokenStart, length)) {
                return CURRENT_TIMESTAMP;
            }
        }
        return IDENTIFIER;
    }

    private static boolean eqCurrent(String expected, String s, int start, int length) {
        for (int i = 8; i < length; i++) {
            if (expected.charAt(i) != (s.charAt(start++) & 0xffdf)) {
                return false;
            }
        }
        return true;
    }

    private int readD(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("DAY", sql, tokenStart, length)) {
            type = DAY;
        } else if (eq("DEFAULT", sql, tokenStart, length)) {
            type = DEFAULT;
        } else if (eq("DISTINCT", sql, tokenStart, length)) {
            type = DISTINCT;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readE(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("ELSE", sql, tokenStart, length)) {
            type = ELSE;
        } else if (eq("END", sql, tokenStart, length)) {
            type = END;
        } else if (eq("EXCEPT", sql, tokenStart, length)) {
            type = EXCEPT;
        } else if (eq("EXISTS", sql, tokenStart, length)) {
            type = EXISTS;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readF(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("FETCH", sql, tokenStart, length)) {
            type = FETCH;
        } else if (eq("FROM", sql, tokenStart, length)) {
            type = FROM;
        } else if (eq("FOR", sql, tokenStart, length)) {
            type = FOR;
        } else if (eq("FOREIGN", sql, tokenStart, length)) {
            type = FOREIGN;
        } else if (eq("FULL", sql, tokenStart, length)) {
            type = FULL;
        } else if (eq("FALSE", sql, tokenStart, length)) {
            type = FALSE;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readG(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("GROUP", sql, tokenStart, length) ? GROUP : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readH(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("HAVING", sql, tokenStart, length)) {
            type = HAVING;
        } else if (eq("HOUR", sql, tokenStart, length)) {
            type = HOUR;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readI(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (length == 2) {
            switch ((sql.charAt(tokenStart + 1) & 0xffdf)) {
            case 'F':
                type = IF;
                break;
            case 'N':
                type = IN;
                break;
            case 'S':
                type = IS;
                break;
            default:
                type = IDENTIFIER;
            }
        } else {
            if (eq("INNER", sql, tokenStart, length)) {
                type = INNER;
            } else if (eq("INTERSECT", sql, tokenStart, length)) {
                type = INTERSECT;
            } else if (eq("INTERVAL", sql, tokenStart, length)) {
                type = INTERVAL;
            } else {
                type = IDENTIFIER;
            }
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readJ(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("JOIN", sql, tokenStart, length) ? JOIN : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readK(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("KEY", sql, tokenStart, length) ? KEY : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readL(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("LEFT", sql, tokenStart, length)) {
            type = LEFT;
        } else if (eq("LIMIT", sql, tokenStart, length)) {
            type = provider.getMode().limit ? LIMIT : IDENTIFIER;
        } else if (eq("LIKE", sql, tokenStart, length)) {
            type = LIKE;
        } else if (eq("LOCALTIME", sql, tokenStart, length)) {
            type = LOCALTIME;
        } else if (eq("LOCALTIMESTAMP", sql, tokenStart, length)) {
            type = LOCALTIMESTAMP;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readM(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("MINUS", sql, tokenStart, length)) {
            type = provider.getMode().minusIsExcept ? MINUS : IDENTIFIER;
        } else if (eq("MINUTE", sql, tokenStart, length)) {
            type = MINUTE;
        } else if (eq("MONTH", sql, tokenStart, length)) {
            type = MONTH;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readN(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("NOT", sql, tokenStart, length)) {
            type = NOT;
        } else if (eq("NATURAL", sql, tokenStart, length)) {
            type = NATURAL;
        } else if (eq("NULL", sql, tokenStart, length)) {
            type = NULL;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readO(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (length == 2) {
            switch ((sql.charAt(tokenStart + 1) & 0xffdf)) {
            case 'N':
                type = ON;
                break;
            case 'R':
                type = OR;
                break;
            default:
                type = IDENTIFIER;
            }
        } else {
            if (eq("OFFSET", sql, tokenStart, length)) {
                type = OFFSET;
            } else if (eq("ORDER", sql, tokenStart, length)) {
                type = ORDER;
            } else {
                type = IDENTIFIER;
            }
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readP(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("PRIMARY", sql, tokenStart, length) ? PRIMARY : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readQ(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("QUALIFY", sql, tokenStart, length) ? QUALIFY : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readR(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("RIGHT", sql, tokenStart, length)) {
            type = RIGHT;
        } else if (eq("ROW", sql, tokenStart, length)) {
            type = ROW;
        } else if (eq("ROWNUM", sql, tokenStart, length)) {
            type = ROWNUM;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readS(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("SECOND", sql, tokenStart, length)) {
            type = SECOND;
        } else if (eq("SELECT", sql, tokenStart, length)) {
            type = SELECT;
        } else if (eq("SESSION_USER", sql, tokenStart, length)) {
            type = SESSION_USER;
        } else if (eq("SET", sql, tokenStart, length)) {
            type = SET;
        } else if (eq("SOME", sql, tokenStart, length)) {
            type = SOME;
        } else if (eq("SYMMETRIC", sql, tokenStart, length)) {
            type = SYMMETRIC;
        } else if (eq("SYSTEM_USER", sql, tokenStart, length)) {
            type = SYSTEM_USER;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readT(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (length == 2) {
            type = (sql.charAt(tokenStart + 1) & 0xffdf) == 'O' ? TO : IDENTIFIER;
        } else {
            if (eq("TABLE", sql, tokenStart, length)) {
                type = TABLE;
            } else if (eq("TRUE", sql, tokenStart, length)) {
                type = TRUE;
            } else {
                type = IDENTIFIER;
            }
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readU(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("UESCAPE", sql, tokenStart, length)) {
            type = UESCAPE;
        } else if (eq("UNION", sql, tokenStart, length)) {
            type = UNION;
        } else if (eq("UNIQUE", sql, tokenStart, length)) {
            type = UNIQUE;
        } else if (eq("UNKNOWN", sql, tokenStart, length)) {
            type = UNKNOWN;
        } else if (eq("USER", sql, tokenStart, length)) {
            type = USER;
        } else if (eq("USING", sql, tokenStart, length)) {
            type = USING;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readV(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("VALUE", sql, tokenStart, length)) {
            type = VALUE;
        } else if (eq("VALUES", sql, tokenStart, length)) {
            type = VALUES;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readW(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type;
        if (eq("WHEN", sql, tokenStart, length)) {
            type = WHEN;
        } else if (eq("WHERE", sql, tokenStart, length)) {
            type = WHERE;
        } else if (eq("WINDOW", sql, tokenStart, length)) {
            type = WINDOW;
        } else if (eq("WITH", sql, tokenStart, length)) {
            type = WITH;
        } else {
            type = IDENTIFIER;
        }
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readY(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int length = endIndex - tokenStart;
        int type = eq("YEAR", sql, tokenStart, length) ? YEAR : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int read_(String sql, int end, int tokenStart, int i, ArrayList<Token> tokens) {
        int endIndex = findIdentifierEnd(sql, end, i);
        int type = endIndex - tokenStart == 7 && "_ROWID_".regionMatches(true, 1, sql, tokenStart + 1, 6) ? _ROWID_
                : IDENTIFIER;
        return readIdentifierOrKeyword(sql, tokenStart, tokens, endIndex, type);
    }

    private int readIdentifierOrKeyword(String sql, int tokenStart, ArrayList<Token> tokens, int endIndex, int type) {
        Token token;
        if (type == IDENTIFIER) {
            token = new Token.IdentifierToken(tokenStart, extractIdentifier(sql, tokenStart, endIndex), false, false);
        } else if (nonKeywords != null && nonKeywords.get(type)) {
            token = new Token.KeywordOrIdentifierToken(tokenStart, type, extractIdentifier(sql, tokenStart, endIndex));
        } else {
            token = new Token.KeywordToken(tokenStart, type);
        }
        tokens.add(token);
        return endIndex;
    }

    private static boolean eq(String expected, String s, int start, int length) {
        if (length != expected.length()) {
            return false;
        }
        for (int i = 1; i < length; i++) {
            if (expected.charAt(i) != (s.charAt(++start) & 0xffdf)) {
                return false;
            }
        }
        return true;
    }

    private int findIdentifierEnd(String sql, int end, int i) {
        i++;
        for (;;) {
            int cp;
            if (i > end || (!Character.isJavaIdentifierPart(cp = sql.codePointAt(i))
                    && (cp != '#' || !provider.getMode().supportPoundSymbolForColumnNames))) {
                break;
            }
            i += Character.charCount(cp);
        }
        return i;
    }

    private String extractIdentifier(String sql, int beginIndex, int endIndex) {
        return convertCase(sql.substring(beginIndex, endIndex));
    }

    private int readQuotedIdentifier(String sql, int end, int tokenStart, int i, char c, boolean unicode,
            ArrayList<Token> tokens) {
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
            s = convertCase(s);
        }
        tokens.add(new Token.IdentifierToken(tokenStart, s, true, unicode));
        return i;
    }

    private String convertCase(String s) {
        if (identifiersToUpper) {
            s = StringUtils.toUpperEnglish(s);
        } else if (identifiersToLower) {
            s = StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    private static int readBinaryString(String sql, int tokenStart, int end, int i, ArrayList<Token> tokens) {
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

    private static int readCharacterString(String sql, int tokenStart, int end, int i, boolean unicode,
            ArrayList<Token> tokens) {
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

    private static int assignParameterIndex(ArrayList<Token> tokens, int lastParameter) {
        Token.ParameterToken parameter = (Token.ParameterToken) tokens.get(tokens.size() - 1);
        if (parameter.index == 0) {
            if (lastParameter < 0) {
                throw DbException.get(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
            }
            parameter.index = ++lastParameter;
        } else if (lastParameter > 0) {
            throw DbException.get(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS);
        } else {
            lastParameter = -1;
        }
        return lastParameter;
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
