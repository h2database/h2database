/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Pattern matching comparison expression: WHERE NAME LIKE ?
 */
public class CompareLike extends Condition {

    private static final int MATCH = 0, ONE = 1, ANY = 2;

    private final CompareMode compareMode;
    private final boolean regexp;
    private Expression left;
    private Expression right;
    private Expression escape;

    private boolean isInit;
    private char[] pattern;
    private String patternString;
    private Pattern patternRegexp;
    private int[] types;
    private int patternLength;
    private boolean ignoreCase;
    private boolean fastCompare;

    public CompareLike(CompareMode compareMode, Expression left, Expression right, Expression escape, boolean regexp) {
        this.compareMode = compareMode;
        this.regexp = regexp;
        this.left = left;
        this.right = right;
        this.escape = escape;
    }

    public String getSQL() {
        String sql;
        if (regexp) {
            sql = left.getSQL() + " REGEXP " + right.getSQL();
        } else {
            sql = left.getSQL() + " LIKE " + right.getSQL();
            if (escape != null) {
                sql += " ESCAPE " + escape.getSQL();
            }
        }
        return "(" + sql + ")";
    }

    public Expression optimize(Session session) throws SQLException {
        left = left.optimize(session);
        right = right.optimize(session);
        if (left.getType() == Value.STRING_IGNORECASE) {
            ignoreCase = true;
        }
        if (left.isValueSet()) {
            Value l = left.getValue(session);
            if (l == ValueNull.INSTANCE) {
                // NULL LIKE something > NULL
                return ValueExpression.getNull();
            }
        }
        if (escape != null) {
            escape = escape.optimize(session);
        }
        if (right.isValueSet() && (escape == null || escape.isValueSet())) {
            if (left.isValueSet()) {
                return ValueExpression.get(getValue(session));
            }
            Value r = right.getValue(session);
            if (r == ValueNull.INSTANCE) {
                // something LIKE NULL > NULL
                return ValueExpression.getNull();
            }
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                return ValueExpression.getNull();
            }
            String p = r.getString();
            initPattern(p, getEscapeChar(e));
            if ("%".equals(p)) {
                // optimization for X LIKE '%': convert to X IS NOT NULL
                return new Comparison(session, Comparison.IS_NOT_NULL, left, null).optimize(session);
            }
            if (isFullMatch()) {
                // optimization for X LIKE 'Hello': convert to X = 'Hello'
                Value value = ValueString.get(patternString);
                Expression expr = ValueExpression.get(value);
                return new Comparison(session, Comparison.EQUAL, left, expr).optimize(session);
            }
            isInit = true;
        }
        return this;
    }

    private Character getEscapeChar(Value e) {
        if (e == null) {
            return SysProperties.DEFAULT_ESCAPE_CHAR;
        }
        String es = e.getString();
        Character esc;
        if (es == null) {
            esc = SysProperties.DEFAULT_ESCAPE_CHAR;
        } else if (es.length() == 0) {
            esc = null;
        } else {
            esc = es.charAt(0);
        }
        return esc;
    }

    public void createIndexConditions(Session session, TableFilter filter) throws SQLException {
        if (regexp) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        // parameters are always evaluatable, but
        // we need to check the actual value now
        // (at prepare time)
        // otherwise we would need to prepare at execute time,
        // which is maybe slower (but maybe not in this case!)
        // TODO optimizer: like: check what other databases do
        if (!right.isValueSet()) {
            return;
        }
        if (escape != null && !escape.isValueSet()) {
            return;
        }
        String p = right.getValue(session).getString();
        Value e = escape == null ? null : escape.getValue(session);
        if (e == ValueNull.INSTANCE) {
            // should already be optimized
            Message.throwInternalError();
        }
        initPattern(p, getEscapeChar(e));
        if (patternLength <= 0 || types[0] != MATCH) {
            // can't use an index
            return;
        }
        int dataType = l.getColumn().getType();
        if (dataType != Value.STRING && dataType != Value.STRING_IGNORECASE && dataType != Value.STRING_FIXED) {
            // column is not a varchar - can't use the index
            return;
        }
        int maxMatch = 0;
        StringBuilder buff = new StringBuilder();
        while (maxMatch < patternLength && types[maxMatch] == MATCH) {
            buff.append(pattern[maxMatch++]);
        }
        String begin = buff.toString();
        if (maxMatch == patternLength) {
            filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL, l, ValueExpression
                    .get(ValueString.get(begin))));
        } else {
            // TODO check if this is correct according to Unicode rules (code
            // points)
            String end;
            if (begin.length() > 0) {
                filter.addIndexCondition(IndexCondition.get(Comparison.BIGGER_EQUAL, l, ValueExpression.get(ValueString
                        .get(begin))));
                char next = begin.charAt(begin.length() - 1);
                // search the 'next' unicode character (or at least a character
                // that is higher)
                for (int i = 1; i < 2000; i++) {
                    end = begin.substring(0, begin.length() - 1) + (char) (next + i);
                    if (compareMode.compareString(begin, end, ignoreCase) == -1) {
                        filter.addIndexCondition(IndexCondition.get(Comparison.SMALLER, l, ValueExpression
                                .get(ValueString.get(end))));
                        break;
                    }
                }
            }
        }
    }

    public Value getValue(Session session) throws SQLException {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        if (!isInit) {
            Value r = right.getValue(session);
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            String p = r.getString();
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            initPattern(p, getEscapeChar(e));
        }
        String value = l.getString();
        boolean result;
        if (regexp) {
            // result = patternRegexp.matcher(value).matches();
            result = patternRegexp.matcher(value).find();
        } else {
            result = compareAt(value, 0, 0, value.length(), pattern, types);
        }
        return ValueBoolean.get(result);
    }

    private boolean compare(char[] pattern, String s, int pi, int si) {
        return pattern[pi] == s.charAt(si) || (!fastCompare && compareMode.equalsChars(patternString, pi, s, si, ignoreCase));
    }

    private boolean compareAt(String s, int pi, int si, int sLen, char[] pattern, int[] types) {
        for (; pi < patternLength; pi++) {
            switch (types[pi]) {
            case MATCH:
                if ((si >= sLen) || !compare(pattern, s, pi, si++)) {
                    return false;
                }
                break;
            case ONE:
                if (si++ >= sLen) {
                    return false;
                }
                break;
            case ANY:
                if (++pi >= patternLength) {
                    return true;
                }
                while (si < sLen) {
                    if (compare(pattern, s, pi, si) && compareAt(s, pi, si, sLen, pattern, types)) {
                        return true;
                    }
                    si++;
                }
                return false;
            default:
                Message.throwInternalError();
            }
        }
        return si == sLen;
    }

    /**
     * Test if the value matches the pattern.
     *
     * @param testPattern the pattern
     * @param value the value
     * @param escapeChar the escape character
     * @return true if the value matches
     */
    public boolean test(String testPattern, String value, char escapeChar) throws SQLException {
        initPattern(testPattern, escapeChar);
        return compareAt(value, 0, 0, value.length(), pattern, types);
    }

    private void initPattern(String p, Character escape) throws SQLException {
        if (compareMode.getName().equals(CompareMode.OFF) && !ignoreCase) {
            fastCompare = true;
        }
        if (regexp) {
            patternString = p;
            try {
                if (ignoreCase) {
                    patternRegexp = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
                } else {
                    patternRegexp = Pattern.compile(p);
                }
            } catch (PatternSyntaxException e) {
                throw Message.getSQLException(ErrorCode.LIKE_ESCAPE_ERROR_1, e, p);
            }
            return;
        }
        patternLength = 0;
        if (p == null) {
            types = null;
            pattern = null;
            return;
        }
        int len = p.length();
        pattern = new char[len];
        types = new int[len];
        boolean lastAny = false;
        for (int i = 0; i < len; i++) {
            char c = p.charAt(i);
            int type;
            if (escape != null && escape == c) {
                if (i >= len - 1) {
                    throw Message.getSQLException(ErrorCode.LIKE_ESCAPE_ERROR_1, StringUtils.addAsterisk(p, i));
                }
                c = p.charAt(++i);
                if (c != '_' && c != '%' && c != escape) {
                    throw Message.getSQLException(ErrorCode.LIKE_ESCAPE_ERROR_1, StringUtils.addAsterisk(p, i));
                }
                type = MATCH;
                lastAny = false;
            } else if (c == '%') {
                if (lastAny) {
                    continue;
                }
                type = ANY;
                lastAny = true;
            } else if (c == '_') {
                type = ONE;
            } else {
                type = MATCH;
                lastAny = false;
            }
            types[patternLength] = type;
            pattern[patternLength++] = c;
        }
        for (int i = 0; i < patternLength - 1; i++) {
            if ((types[i] == ANY) && (types[i + 1] == ONE)) {
                types[i] = ONE;
                types[i + 1] = ANY;
            }
        }
        patternString = new String(pattern, 0, patternLength);
    }

    private boolean isFullMatch() {
        if (types == null) {
            return false;
        }
        for (int type : types) {
            if (type != MATCH) {
                return false;
            }
        }
        return true;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
        if (escape != null) {
            escape.mapColumns(resolver, level);
        }
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
        if (escape != null) {
            escape.setEvaluatable(tableFilter, b);
        }
    }

    public void updateAggregate(Session session) throws SQLException {
        left.updateAggregate(session);
        right.updateAggregate(session);
        if (escape != null) {
            escape.updateAggregate(session);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor)
                && (escape == null || escape.isEverything(visitor));
    }

    public int getCost() {
        return left.getCost() + right.getCost() + 3;
    }

}
