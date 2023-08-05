/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import static org.h2.command.Token.CLOSE_PAREN;
import static org.h2.command.Token.COMMA;
import static org.h2.command.Token.LITERAL;
import static org.h2.command.Token.MINUS_SIGN;
import static org.h2.command.Token.OPEN_PAREN;
import static org.h2.command.Token.PLUS_SIGN;
import static org.h2.command.Token.TOKENS;
import static org.h2.util.ParserUtil.FALSE;
import static org.h2.util.ParserUtil.FIRST_KEYWORD;
import static org.h2.util.ParserUtil.IDENTIFIER;
import static org.h2.util.ParserUtil.LAST_KEYWORD;
import static org.h2.util.ParserUtil.ON;
import static org.h2.util.ParserUtil.TRUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.StringJoiner;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbSettings;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.util.HasSQL;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * The base class for the parser.
 */
public class ParserBase {

    /**
     * Add double quotes around an identifier if required.
     *
     * @param s
     *            the identifier
     * @param sqlFlags
     *            formatting flags
     * @return the quoted identifier
     */
    public static String quoteIdentifier(String s, int sqlFlags) {
        if (s == null) {
            return "\"\"";
        }
        if ((sqlFlags & HasSQL.QUOTE_ONLY_WHEN_REQUIRED) != 0 && ParserUtil.isSimpleIdentifier(s, false, false)) {
            return s;
        }
        return StringUtils.quoteIdentifier(s);
    }

    /**
     * Parses the specified collection of non-keywords.
     *
     * @param nonKeywords
     *            array of non-keywords in upper case
     * @return bit set of non-keywords, or {@code null}
     */
    public static BitSet parseNonKeywords(String[] nonKeywords) {
        if (nonKeywords.length == 0) {
            return null;
        }
        BitSet set = new BitSet();
        for (String nonKeyword : nonKeywords) {
            int index = Arrays.binarySearch(TOKENS, FIRST_KEYWORD, LAST_KEYWORD + 1, nonKeyword);
            if (index >= 0) {
                set.set(index);
            }
        }
        return set.isEmpty() ? null : set;
    }

    /**
     * Formats a comma-separated list of keywords.
     *
     * @param nonKeywords
     *            bit set of non-keywords, or {@code null}
     * @return comma-separated list of non-keywords
     */
    public static String formatNonKeywords(BitSet nonKeywords) {
        if (nonKeywords == null || nonKeywords.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = -1; (i = nonKeywords.nextSetBit(i + 1)) >= 0;) {
            if (i >= FIRST_KEYWORD && i <= LAST_KEYWORD) {
                if (builder.length() > 0) {
                    builder.append(',');
                }
                builder.append(TOKENS[i]);
            }
        }
        return builder.toString();
    }

    static boolean isKeyword(int tokenType) {
        return tokenType >= FIRST_KEYWORD && tokenType <= LAST_KEYWORD;
    }

    /**
     * The database or {@code null}.
     */
    final Database database;

    /**
     * The session or {@code null}.
     */
    final SessionLocal session;

    /**
     * @see org.h2.engine.DbSettings#databaseToLower
     */
    private final boolean identifiersToLower;

    /**
     * @see org.h2.engine.DbSettings#databaseToUpper
     */
    private final boolean identifiersToUpper;

    /**
     * @see org.h2.engine.SessionLocal#isVariableBinary()
     */
    final boolean variableBinary;

    /**
     * Value of NON_KEYWORDS setting.
     */
    final BitSet nonKeywords;

    /**
     * Tokens.
     */
    ArrayList<Token> tokens;

    /**
     * Index of the current token.
     */
    int tokenIndex;

    /**
     * The current token.
     */
    Token token;

    /**
     * Type of the current token.
     */
    int currentTokenType;

    /**
     * String representation of the current token.
     */
    String currentToken;

    /**
     * Original SQL.
     */
    String sqlCommand;

    /**
     * JDBC parameters.
     */
    ArrayList<Parameter> parameters;

    /**
     * Indexes of used parameters.
     */
    BitSet usedParameters = new BitSet();

    /**
     * If {@code true}, checks for literals are disabled.
     */
    private boolean literalsChecked;

    /**
     * List of expected tokens or {@code null}.
     */
    ArrayList<String> expectedList;

    ParserBase(SessionLocal session) {
        this.database = session.getDatabase();
        DbSettings settings = database.getSettings();
        this.identifiersToLower = settings.databaseToLower;
        this.identifiersToUpper = settings.databaseToUpper;
        this.variableBinary = session.isVariableBinary();
        this.nonKeywords = session.getNonKeywords();
        this.session = session;
    }

    /**
     * Creates a new instance of parser for special use cases.
     */
    public ParserBase() {
        database = null;
        identifiersToLower = false;
        identifiersToUpper = false;
        variableBinary = false;
        nonKeywords = null;
        session = null;
    }

    public final void setLiteralsChecked(boolean literalsChecked) {
        this.literalsChecked = literalsChecked;
    }

    public final void setSuppliedParameters(ArrayList<Parameter> suppliedParameters) {
        this.parameters = suppliedParameters;
    }

    /**
     * Parses a list of column names or numbers in parentheses.
     *
     * @param sql
     *            the source SQL
     * @param offset
     *            the initial offset
     * @return the array of column names ({@code String[]}) or numbers
     *         ({@code int[]})
     * @throws DbException
     *             on syntax error
     */
    public Object parseColumnList(String sql, int offset) {
        initialize(sql, null, true);
        for (int i = 0, l = tokens.size(); i < l; i++) {
            if (tokens.get(i).start() >= offset) {
                setTokenIndex(i);
                break;
            }
        }
        read(OPEN_PAREN);
        if (readIf(CLOSE_PAREN)) {
            return Utils.EMPTY_INT_ARRAY;
        }
        if (isIdentifier()) {
            ArrayList<String> list = Utils.newSmallArrayList();
            do {
                if (!isIdentifier()) {
                    throw getSyntaxError();
                }
                list.add(currentToken);
                read();
            } while (readIfMore());
            return list.toArray(new String[0]);
        } else if (currentTokenType == LITERAL) {
            ArrayList<Integer> list = Utils.newSmallArrayList();
            do {
                list.add(readInt());
            } while (readIfMore());
            int count = list.size();
            int[] array = new int[count];
            for (int i = 0; i < count; i++) {
                array[i] = list.get(i);
            }
            return array;
        } else {
            throw getSyntaxError();
        }
    }

    final void initialize(String sql, ArrayList<Token> tokens, boolean stopOnCloseParen) {
        if (sql == null) {
            sql = "";
        }
        sqlCommand = sql;
        if (tokens == null) {
            BitSet usedParameters = new BitSet();
            this.tokens = new Tokenizer(database, identifiersToUpper, identifiersToLower, nonKeywords).tokenize(sql,
                    stopOnCloseParen, usedParameters);
            if (parameters == null) {
                int l = usedParameters.length();
                if (l > Constants.MAX_PARAMETER_INDEX) {
                    throw DbException.getInvalidValueException("parameter index", l);
                }
                if (l > 0) {
                    parameters = new ArrayList<>(l);
                    for (int i = 0; i < l; i++) {
                        /*
                         * We need to create parameters even when they aren't
                         * actually used, for example, VALUES ?1, ?3 needs
                         * parameters ?1, ?2, and ?3.
                         */
                        parameters.add(new Parameter(i));
                    }
                } else {
                    parameters = new ArrayList<>();
                }
            }
        } else {
            this.tokens = tokens;
        }
        resetTokenIndex();
    }

    final void resetTokenIndex() {
        tokenIndex = -1;
        token = null;
        currentTokenType = -1;
        currentToken = null;
    }

    final void setTokenIndex(int index) {
        if (index != tokenIndex) {
            if (expectedList != null) {
                expectedList.clear();
            }
            token = tokens.get(index);
            tokenIndex = index;
            currentTokenType = token.tokenType();
            currentToken = token.asIdentifier();
        }
    }

    final BitSet openParametersScope() {
        BitSet outerUsedParameters = usedParameters;
        usedParameters = new BitSet();
        return outerUsedParameters;
    }

    final ArrayList<Parameter> closeParametersScope(BitSet outerUsedParameters) {
        BitSet innerUsedParameters = usedParameters;
        int size = innerUsedParameters.cardinality();
        ArrayList<Parameter> params = new ArrayList<>(size);
        if (size > 0) {
            for (int i = -1; (i = innerUsedParameters.nextSetBit(i + 1)) >= 0;) {
                params.add(parameters.get(i));
            }
        }
        outerUsedParameters.or(innerUsedParameters);
        usedParameters = outerUsedParameters;
        return params;
    }

    final void read(String expected) {
        if (!testToken(expected, token)) {
            addExpected(expected);
            throw getSyntaxError();
        }
        read();
    }

    final void read(int tokenType) {
        if (tokenType != currentTokenType) {
            addExpected(tokenType);
            throw getSyntaxError();
        }
        read();
    }

    final void readCompat(int tokenType) {
        if (tokenType != currentTokenType) {
            throw getSyntaxError();
        }
        read();
    }

    final boolean readIf(String tokenName) {
        if (testToken(tokenName, token)) {
            read();
            return true;
        }
        addExpected(tokenName);
        return false;
    }

    final boolean readIfCompat(String tokenName) {
        if (testToken(tokenName, token)) {
            read();
            return true;
        }
        return false;
    }

    final boolean readIf(String tokenName1, String tokenName2) {
        int i = tokenIndex + 1;
        if (i + 1 < tokens.size() && testToken(tokenName1, token) && testToken(tokenName2, tokens.get(i))) {
            setTokenIndex(i + 1);
            return true;
        }
        addExpected(tokenName1, tokenName2);
        return false;
    }

    final boolean readIf(String tokenName1, int tokenType2) {
        int i = tokenIndex + 1;
        if (i + 1 < tokens.size() && tokens.get(i).tokenType() == tokenType2 && testToken(tokenName1, token)) {
            setTokenIndex(i + 1);
            return true;
        }
        addExpected(tokenName1, TOKENS[tokenType2]);
        return false;
    }

    final boolean readIf(int tokenType) {
        if (tokenType == currentTokenType) {
            read();
            return true;
        }
        addExpected(tokenType);
        return false;
    }

    final boolean readIfCompat(int tokenType) {
        if (tokenType == currentTokenType) {
            read();
            return true;
        }
        return false;
    }

    final boolean readIf(int tokenType1, int tokenType2) {
        if (tokenType1 == currentTokenType) {
            int i = tokenIndex + 1;
            if (tokens.get(i).tokenType() == tokenType2) {
                setTokenIndex(i + 1);
                return true;
            }
        }
        addExpected(tokenType1, tokenType2);
        return false;
    }

    final boolean readIfCompat(int tokenType1, int tokenType2) {
        if (tokenType1 == currentTokenType) {
            int i = tokenIndex + 1;
            if (tokens.get(i).tokenType() == tokenType2) {
                setTokenIndex(i + 1);
                return true;
            }
        }
        return false;
    }

    final boolean readIf(int tokenType1, String tokenName2) {
        if (tokenType1 == currentTokenType) {
            int i = tokenIndex + 1;
            if (testToken(tokenName2, tokens.get(i))) {
                setTokenIndex(i + 1);
                return true;
            }
        }
        addExpected(TOKENS[tokenType1], tokenName2);
        return false;
    }

    final boolean readIfCompat(int tokenType1, String tokenName2) {
        if (tokenType1 == currentTokenType) {
            int i = tokenIndex + 1;
            if (testToken(tokenName2, tokens.get(i))) {
                setTokenIndex(i + 1);
                return true;
            }
        }
        return false;
    }

    final boolean readIf(Object... tokensTypesOrNames) {
        int count = tokensTypesOrNames.length;
        int size = tokens.size();
        int i = tokenIndex;
        check: if (i + count < size) {
            for (Object tokenTypeOrName : tokensTypesOrNames) {
                if (!testToken(tokenTypeOrName, tokens.get(i++))) {
                    break check;
                }
            }
            setTokenIndex(i);
            return true;
        }
        addExpected(tokensTypesOrNames);
        return false;
    }

    final boolean readIfCompat(Object... tokensTypesOrNames) {
        int count = tokensTypesOrNames.length;
        int size = tokens.size();
        int i = tokenIndex;
        check: if (i + count < size) {
            for (Object tokenTypeOrName : tokensTypesOrNames) {
                if (!testToken(tokenTypeOrName, tokens.get(i++))) {
                    break check;
                }
            }
            setTokenIndex(i);
            return true;
        }
        return false;
    }

    final boolean isToken(String tokenName) {
        if (testToken(tokenName, token)) {
            return true;
        }
        addExpected(tokenName);
        return false;
    }

    final boolean isTokenCompat(String tokenName) {
        return testToken(tokenName, token);
    }

    private boolean testToken(Object expected, Token token) {
        return expected instanceof Integer ? (int) expected == token.tokenType() : testToken((String) expected, token);
    }

    private boolean testToken(String tokenName, Token token) {
        if (!token.isQuoted()) {
            String s = token.asIdentifier();
            return identifiersToUpper ? tokenName.equals(s) : tokenName.equalsIgnoreCase(s);
        }
        return false;
    }

    final boolean isToken(int tokenType) {
        if (tokenType == currentTokenType) {
            return true;
        }
        addExpected(tokenType);
        return false;
    }

    final boolean equalsToken(String a, String b) {
        if (a == null) {
            return b == null;
        } else
            return a.equals(b) || !identifiersToUpper && a.equalsIgnoreCase(b);
    }

    final boolean isIdentifier() {
        return currentTokenType == IDENTIFIER || nonKeywords != null && nonKeywords.get(currentTokenType);
    }

    final void addExpected(String token) {
        if (expectedList != null) {
            expectedList.add(token);
        }
    }

    final void addExpected(int tokenType) {
        if (expectedList != null) {
            expectedList.add(TOKENS[tokenType]);
        }
    }

    private void addExpected(int tokenType1, int tokenType2) {
        if (expectedList != null) {
            expectedList.add(TOKENS[tokenType1] + ' ' + TOKENS[tokenType2]);
        }
    }

    private void addExpected(String tokenType1, String tokenType2) {
        if (expectedList != null) {
            expectedList.add(tokenType1 + ' ' + tokenType2);
        }
    }

    private void addExpected(Object... tokens) {
        if (expectedList != null) {
            StringJoiner j = new StringJoiner(" ");
            for (Object token : tokens) {
                j.add(token instanceof Integer ? TOKENS[(int) token] : (String) token);
            }
            expectedList.add(j.toString());
        }
    }

    final void addMultipleExpected(int... tokenTypes) {
        for (int tokenType : tokenTypes) {
            expectedList.add(TOKENS[tokenType]);
        }
    }

    final void read() {
        if (expectedList != null) {
            expectedList.clear();
        }
        int size = tokens.size();
        if (tokenIndex + 1 < size) {
            token = tokens.get(++tokenIndex);
            currentTokenType = token.tokenType();
            currentToken = token.asIdentifier();
            if (currentToken != null && currentToken.length() > Constants.MAX_IDENTIFIER_LENGTH) {
                throw DbException.get(ErrorCode.NAME_TOO_LONG_2, currentToken.substring(0, 32),
                        "" + Constants.MAX_IDENTIFIER_LENGTH);
            } else if (currentTokenType == LITERAL) {
                checkLiterals();
            }
        } else {
            throw getSyntaxError();
        }
    }

    private void checkLiterals() {
        if (!literalsChecked && session != null && !session.getAllowLiterals()) {
            int allowed = database.getAllowLiterals();
            if (allowed == Constants.ALLOW_LITERALS_NONE
                    || ((token instanceof Token.CharacterStringToken || token instanceof Token.BinaryStringToken)
                            && allowed != Constants.ALLOW_LITERALS_ALL)) {
                throw DbException.get(ErrorCode.LITERALS_ARE_NOT_ALLOWED);
            }
        }
    }

    /**
     * Read comma or closing brace.
     *
     * @return {@code true} if comma is read, {@code false} if brace is read
     */
    final boolean readIfMore() {
        if (readIf(COMMA)) {
            return true;
        }
        read(CLOSE_PAREN);
        return false;
    }

    final int readNonNegativeInt() {
        int v = readInt();
        if (v < 0) {
            throw DbException.getInvalidValueException("non-negative integer", v);
        }
        return v;
    }

    final int readInt() {
        boolean minus = false;
        if (currentTokenType == MINUS_SIGN) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS_SIGN) {
            read();
        }
        if (currentTokenType != LITERAL) {
            throw DbException.getSyntaxError(sqlCommand, token.start(), "integer");
        }
        Value value = token.value(session);
        if (minus) {
            // must do that now, otherwise Integer.MIN_VALUE would not work
            value = value.negate();
        }
        int i = value.getInt();
        read();
        return i;
    }

    final long readPositiveLong() {
        long v = readLong();
        if (v <= 0) {
            throw DbException.getInvalidValueException("positive long", v);
        }
        return v;
    }

    final long readLong() {
        boolean minus = false;
        if (currentTokenType == MINUS_SIGN) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS_SIGN) {
            read();
        }
        if (currentTokenType != LITERAL) {
            throw DbException.getSyntaxError(sqlCommand, token.start(), "long");
        }
        Value value = token.value(session);
        if (minus) {
            // must do that now, otherwise Long.MIN_VALUE would not work
            value = value.negate();
        }
        long i = value.getLong();
        read();
        return i;
    }

    final boolean readBooleanSetting() {
        switch (currentTokenType) {
        case ON:
        case TRUE:
            read();
            return true;
        case FALSE:
            read();
            return false;
        case LITERAL:
            boolean result = token.value(session).getBoolean();
            read();
            return result;
        }
        if (readIf("OFF")) {
            return false;
        } else {
            if (expectedList != null) {
                addMultipleExpected(ON, TRUE, FALSE);
            }
            throw getSyntaxError();
        }
    }

    final Parameter readParameter() {
        int index = ((Token.ParameterToken) token).index() - 1;
        read();
        usedParameters.set(index);
        return parameters.get(index);
    }

    final boolean isKeyword(String s) {
        return ParserUtil.isKeyword(s, !identifiersToUpper);
    }

    final String upperName(String name) {
        return identifiersToUpper ? name : StringUtils.toUpperEnglish(name);
    }

    /**
     * Returns the last parse index.
     *
     * @return the last parse index
     */
    public final int getLastParseIndex() {
        return token.start();
    }

    final ArrayList<Token> getRemainingTokens(int offset) {
        List<Token> subList = tokens.subList(tokenIndex, tokens.size());
        ArrayList<Token> remainingTokens = new ArrayList<>(subList);
        subList.clear();
        tokens.add(new Token.EndOfInputToken(offset));
        for (Token token : remainingTokens) {
            token.subtractFromStart(offset);
        }
        return remainingTokens;
    }

    final DbException getSyntaxError() {
        if (expectedList == null || expectedList.isEmpty()) {
            return DbException.getSyntaxError(sqlCommand, token.start());
        }
        return DbException.getSyntaxError(sqlCommand, token.start(), String.join(", ", expectedList));
    }

    @Override
    public final String toString() {
        return StringUtils.addAsterisk(sqlCommand, token.start());
    }

}
