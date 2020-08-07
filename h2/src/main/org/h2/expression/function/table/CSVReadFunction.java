/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.table;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.function.CSVWriteFunction;
import org.h2.expression.function.FunctionCall;
import org.h2.expression.function.FunctionN;
import org.h2.message.DbException;
import org.h2.tools.Csv;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueResultSet;

/**
 * A CSVREAD function.
 */
public final class CSVReadFunction extends FunctionN implements FunctionCall {

    public CSVReadFunction() {
        super(new Expression[4]);
    }

    @Override
    public Value getValue(SessionLocal session) {
        session.getUser().checkAdmin();
        String fileName = getValue(session, 0);
        String columnList = getValue(session, 1);
        Csv csv = new Csv();
        String options = getValue(session, 2);
        String charset = null;
        if (options != null && options.indexOf('=') >= 0) {
            charset = csv.setOptions(options);
        } else {
            charset = options;
            String fieldSeparatorRead = getValue(session, 3);
            String fieldDelimiter = getValue(session, 4);
            String escapeCharacter = getValue(session, 5);
            String nullString = getValue(session, 6);
            CSVWriteFunction.setCsvDelimiterEscape(csv, fieldSeparatorRead, fieldDelimiter, escapeCharacter);
            csv.setNullString(nullString);
        }
        char fieldSeparator = csv.getFieldSeparatorRead();
        String[] columns = StringUtils.arraySplit(columnList, fieldSeparator, true);
        try {
            return ValueResultSet.get(session, csv.read(fileName, columns, charset), Integer.MAX_VALUE);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private String getValue(SessionLocal session, int index) {
        return getValue(session, args, index);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        optimizeArguments(session, false);
        int len = args.length;
        if (len < 1 || len > 7) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, getName(), "1..7");
        }
        type = TypeInfo.TYPE_RESULT_SET;
        return this;
    }

    @Override
    public ValueResultSet getValueForColumnList(SessionLocal session, Expression[] nullArgs) {
        session.getUser().checkAdmin();
        String fileName = getValue(session, nullArgs, 0);
        if (fileName == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "fileName");
        }
        String columnList = getValue(session, nullArgs, 1);
        Csv csv = new Csv();
        String options = getValue(session, nullArgs, 2);
        String charset = null;
        if (options != null && options.indexOf('=') >= 0) {
            charset = csv.setOptions(options);
        } else {
            charset = options;
            String fieldSeparatorRead = getValue(session, nullArgs, 3);
            String fieldDelimiter = getValue(session, nullArgs, 4);
            String escapeCharacter = getValue(session, nullArgs, 5);
            CSVWriteFunction.setCsvDelimiterEscape(csv, fieldSeparatorRead, fieldDelimiter, escapeCharacter);
        }
        char fieldSeparator = csv.getFieldSeparatorRead();
        String[] columns = StringUtils.arraySplit(columnList, fieldSeparator, true);
        ValueResultSet x;
        try (ResultSet rs = csv.read(fileName, columns, charset)) {
            x = ValueResultSet.get(session, rs, 0);
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            csv.close();
        }
        return x;
    }

    private static String getValue(SessionLocal session, Expression[] args, int index) {
        return index < args.length ? args[index].getValue(session).getString() : null;
    }

    @Override
    public Expression[] getExpressionColumns(SessionLocal session) {
        return getExpressionColumns(session, getValueForColumnList(session, null).getResult());
    }

    @Override
    public String getName() {
        return "CSVREAD";
    }

    @Override
    public Expression[] getArgs() {
        return args;
    }

    @Override
    public int getValueType() {
        return Value.RESULT_SET;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return isEverythingNonDeterministic(visitor);
    }

}
