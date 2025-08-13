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
import static org.h2.util.json.path.PathToken.FIRST_KEYWORD;
import static org.h2.util.json.path.PathToken.FLAG;
import static org.h2.util.json.path.PathToken.FLOOR;
import static org.h2.util.json.path.PathToken.GREATER_THAN_OPERATOR;
import static org.h2.util.json.path.PathToken.GREATER_THAN_OR_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.IS;
import static org.h2.util.json.path.PathToken.KEYVALUE;
import static org.h2.util.json.path.PathToken.KEY_NAME;
import static org.h2.util.json.path.PathToken.LAST;
import static org.h2.util.json.path.PathToken.LAST_KEYWORD;
import static org.h2.util.json.path.PathToken.LAX;
import static org.h2.util.json.path.PathToken.LEFT_BRACKET;
import static org.h2.util.json.path.PathToken.LEFT_PAREN;
import static org.h2.util.json.path.PathToken.LESS_THAN_OPERATOR;
import static org.h2.util.json.path.PathToken.LESS_THAN_OR_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.LIKE_REGEX;
import static org.h2.util.json.path.PathToken.MINUS_SIGN;
import static org.h2.util.json.path.PathToken.NAMED_VARIABLE;
import static org.h2.util.json.path.PathToken.NOT_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.NULL;
import static org.h2.util.json.path.PathToken.NUMERIC_LITERAL;
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
import static org.h2.util.json.path.PathToken.STRING_LITERAL;
import static org.h2.util.json.path.PathToken.TO;
import static org.h2.util.json.path.PathToken.TRUE;
import static org.h2.util.json.path.PathToken.TYPE;
import static org.h2.util.json.path.PathToken.UNKNOWN;
import static org.h2.util.json.path.PathToken.WITH;

import java.util.ArrayList;

import org.h2.message.DbException;
import org.h2.util.Utils;
import org.h2.util.json.path.PathToken.BigDecimalToken;
import org.h2.util.json.path.PathToken.IntegerToken;

/**
 * SQL/JSON path parser.
 */
public final class PathParser {

    private final String path;

    private final ArrayList<PathToken> tokens;

    private final int size;

    private int tokenIndex;

    private PathToken token;

    private int tokenType;

    private final boolean strict;

    PathParser(String path) {
        this.path = path;
        tokens = PathTokenizer.tokenize(path);
        size = tokens.size();
        token = tokens.get(0);
        tokenType = token.tokenType();
        strict = tokenType == STRICT;
        if (strict || tokenType == LAX) {
            read();
        }
    }

    boolean isStrict() {
        return strict;
    }

    JsonPathExpression parse() {
        return readWFF();
    }

    private JsonPathExpression readWFF() {
        return readAdditiveExpression();
    }

    private JsonPathExpression readAdditiveExpression() {
        JsonPathExpression expression = readMultiplicativeExpression();
        for (int type; (type = tokenType) == PLUS_SIGN || type == MINUS_SIGN;) {
            read();
            expression = new BinaryExpression(expression, readMultiplicativeExpression(), type);
        }
        return expression;
    }

    private JsonPathExpression readMultiplicativeExpression() {
        JsonPathExpression expression = readUnaryExpression();
        for (int type; (type = tokenType) == ASTERISK || type == SOLIDUS || type == PERCENT;) {
            read();
            expression = new BinaryExpression(expression, readUnaryExpression(), type);
        }
        return expression;
    }

    private JsonPathExpression readUnaryExpression() {
        int type = tokenType;
        if (type == PLUS_SIGN || type == MINUS_SIGN) {
            read();
            return new UnaryExpression(readUnaryExpression(), type);
        }
        return readAccessorExpression();
    }

    private JsonPathExpression readAccessorExpression() {
        JsonPathExpression expression = readPathPrimary();
        for (;;) {
            int type = tokenType;
            switch (type) {
            case PERIOD: {
                read();
                type = tokenType;
                if (tokenIndex + 1 < size && tokens.get(tokenIndex + 1).tokenType() == LEFT_PAREN) {
                    expression = readMethod(expression);
                } else if (type == ASTERISK) {
                    read();
                    expression = new MemberAccessorExpression(expression, null);
                } else if (type == KEY_NAME || type == STRING_LITERAL
                        || (type >= FIRST_KEYWORD && type <= LAST_KEYWORD)) {
                    expression = new MemberAccessorExpression(expression, token.asIdentifier());
                    read();
                } else {
                    throw getSyntaxError();
                }
                break;
            }
            case LEFT_BRACKET:
                read();
                type = tokenType;
                ArrayList<JsonPathExpression[]> list = null;
                if (type == ASTERISK) {
                    read();
                } else {
                    list = Utils.newSmallArrayList();
                    do {
                        JsonPathExpression from = readWFF();
                        if (readIf(TO)) {
                            JsonPathExpression to = readWFF();
                            list.add(new JsonPathExpression[] { from, to });
                        } else {
                            list.add(new JsonPathExpression[] { from });
                        }
                    } while (readIf(COMMA));

                }
                read(RIGHT_BRACKET);
                expression = new ArrayAccessorExpression(expression, list);
                break;
            case QUESTION_MARK:
                read();
                read(LEFT_PAREN);
                expression = new FilterExpression(expression, readPathPredicate());
                read(RIGHT_PAREN);
                break;
            default:
                return expression;
            }
        }
    }

    private JsonPathPredicate readPathPredicate() {
        JsonPathPredicate predicate = readBooleanDisjuction();
        return predicate;
    }

    private JsonPathPredicate readBooleanDisjuction() {
        JsonPathPredicate predicate = readBooleanConjunction();
        while (readIf(DOUBLE_VERTICAL_BAR)) {
            predicate = new BinaryPredicate(predicate, readBooleanConjunction(), DOUBLE_VERTICAL_BAR);
        }
        return predicate;
    }

    private JsonPathPredicate readBooleanConjunction() {
        JsonPathPredicate predicate = readBooleanNegation();
        while (readIf(DOUBLE_AMPERSAND)) {
            predicate = new BinaryPredicate(predicate, readBooleanNegation(), DOUBLE_AMPERSAND);
        }
        return predicate;
    }

    private JsonPathPredicate readBooleanNegation() {
        if (readIf(EXCLAMATION_MARK)) {
            return new UnaryPredicate(readDelimitedPredicate(), EXCLAMATION_MARK);
        }
        return readPredicatePrimary();
    }

    private JsonPathPredicate readPredicatePrimary() {
        if (readIf(EXISTS)) {
            read(LEFT_PAREN);
            JsonPathExpression expression = readWFF();
            read(RIGHT_PAREN);
            return new ExistsPathPredicate(expression);
        }
        if (readIf(LEFT_PAREN)) {
            JsonPathPredicate predicate = readPathPredicate();
            read(RIGHT_PAREN);
            if (readIf(IS, UNKNOWN)) {
                return new UnaryPredicate(predicate, UNKNOWN);
            }
            return predicate;
        }
        JsonPathExpression expression = readWFF();
        int type = tokenType;
        switch (type) {
        case DOUBLE_EQUALS:
        case NOT_EQUALS_OPERATOR:
        case LESS_THAN_OPERATOR:
        case GREATER_THAN_OPERATOR:
        case LESS_THAN_OR_EQUALS_OPERATOR:
        case GREATER_THAN_OR_EQUALS_OPERATOR: {
            read();
            JsonPathExpression expression2 = readWFF();
            return new ComparisonPredicate(expression, expression2, type);
        }
        case LIKE_REGEX: {
            read();
            String pattern = readString(), flags = null;
            if (readIf(FLAG)) {
                flags = readString();
            }
            return new LikeRegexPredicate(expression, pattern, flags);
        }
        case STARTS: {
            read();
            read(WITH);
            if (tokenType == STRING_LITERAL) {
                return new StartsWithPredicate(expression, readString(), false);
            } else if (tokenType == NAMED_VARIABLE) {
                String s = token.asIdentifier();
                read();
                return new StartsWithPredicate(expression, s, true);
            }
            break;
        }
        }
        throw getSyntaxError();
    }

    private JsonPathPredicate readDelimitedPredicate() {
        if (readIf(EXISTS)) {
            read(LEFT_PAREN);
            JsonPathExpression expression = readWFF();
            read(RIGHT_PAREN);
            return new ExistsPathPredicate(expression);
        }
        read(LEFT_PAREN);
        JsonPathPredicate predicate = readPathPredicate();
        read(RIGHT_PAREN);
        return predicate;
    }

    private JsonPathExpression readMethod(JsonPathExpression expression) {
        int type = tokenType;
        switch (type) {
        case TYPE:
        case SIZE:
        case DOUBLE:
        case CEILING:
        case FLOOR:
        case ABS:
        case KEYVALUE:
            setTokenIndex(tokenIndex + 2);
            expression = new SimpleMethod(expression, type);
            break;
        case DATETIME: {
            PathToken template = tokens.get(tokenIndex + 2);
            if (template.tokenType() != STRING_LITERAL) {
                throw getSyntaxError();
            }
            setTokenIndex(tokenIndex + 3);
            expression = new DatetimeMethod(expression, template.toString());
            break;
        }
        default:
            throw getSyntaxError();
        }
        read(RIGHT_PAREN);
        return expression;
    }

    private JsonPathExpression readPathPrimary() {
        int type = tokenType;
        switch (type) {
        case TRUE:
            read();
            return LiteralExpression.TRUE;
        case FALSE:
            read();
            return LiteralExpression.FALSE;
        case NULL:
            read();
            return LiteralExpression.NULL;
        case STRING_LITERAL: {
            String s = token.toString();
            read();
            return LiteralExpression.get(s);
        }
        case NUMERIC_LITERAL: {
            JsonPathExpression e = token instanceof BigDecimalToken
                    ? LiteralExpression.get(((BigDecimalToken) token).getNumber())
                    : LiteralExpression.get(((IntegerToken) token).getNumber());
            read();
            return e;
        }
        case DOLLAR_SIGN:
            read();
            return ContextVariableExpression.INSTANCE;
        case NAMED_VARIABLE: {
            String s = token.asIdentifier();
            read();
            return new NamedVariableExpression(s);
        }
        case AT_SIGN:
            read();
            return CurrentItemExpression.INSTANCE;
        case LAST:
            read();
            return LastSubscriptExpression.INSTANCE;
        case LEFT_PAREN: {
            read();
            JsonPathExpression e = readWFF();
            read(RIGHT_PAREN);
            return e;
        }
        default:
            throw getSyntaxError();
        }
    }

    private String readString() {
        if (tokenType != STRING_LITERAL) {
            throw getSyntaxError();
        }
        String s = token.toString();
        read();
        return s;
    }

    private void read(int type) {
        if (tokenType != type) {
            throw getSyntaxError();
        }
        read();
    }

    private boolean readIf(int type) {
        if (tokenType != type) {
            return false;
        }
        read();
        return true;
    }

    private boolean readIf(int type1, int type2) {
        if (tokenType != type1 || tokens.get(tokenIndex + 1).tokenType() != type2) {
            return false;
        }
        setTokenIndex(tokenIndex + 2);
        return true;
    }

    private void read() {
        if (tokenIndex + 1 < size) {
            token = tokens.get(++tokenIndex);
            tokenType = token.tokenType();
        } else {
            throw getSyntaxError();
        }
    }

    private void setTokenIndex(int index) {
        if (index != tokenIndex) {
            tokenIndex = index;
            token = tokens.get(tokenIndex);
            tokenType = token.tokenType();
        }
    }

    private DbException getSyntaxError() {
        return DbException.getSyntaxError(path, token.start());
    }

}
