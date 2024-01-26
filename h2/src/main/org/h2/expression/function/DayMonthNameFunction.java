/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.text.DateFormatSymbols;
import java.util.Locale;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * A DAYNAME() or MONTHNAME() function.
 */
public final class DayMonthNameFunction extends Function1 {

    /**
     * DAYNAME() (non-standard).
     */
    public static final int DAYNAME = 0;

    /**
     * MONTHNAME() (non-standard).
     */
    public static final int MONTHNAME = DAYNAME + 1;

    private static final String[] NAMES = { //
            "DAYNAME", "MONTHNAME" //
    };

    /**
     * English names of months and week days.
     */
    private static volatile String[][] MONTHS_AND_WEEKS;

    private final int function;

    public DayMonthNameFunction(Expression arg, int function) {
        super(arg);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = arg.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        long dateValue = DateTimeUtils.dateAndTimeFromValue(v, session)[0];
        String result;
        switch (function) {
        case DAYNAME:
            result = getMonthsAndWeeks(1)[DateTimeUtils.getDayOfWeek(dateValue, 0)];
            break;
        case MONTHNAME:
            result = getMonthsAndWeeks(0)[DateTimeUtils.monthFromDateValue(dateValue) - 1];
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return ValueVarchar.get(result, session);
    }

    /**
     * Return names of month or weeks.
     *
     * @param field
     *            0 for months, 1 for weekdays
     * @return names of month or weeks
     */
    private static String[] getMonthsAndWeeks(int field) {
        String[][] result = MONTHS_AND_WEEKS;
        if (result == null) {
            result = new String[2][];
            DateFormatSymbols dfs = DateFormatSymbols.getInstance(Locale.ENGLISH);
            result[0] = dfs.getMonths();
            result[1] = dfs.getWeekdays();
            MONTHS_AND_WEEKS = result;
        }
        return result[field];
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        type = TypeInfo.getTypeInfo(Value.VARCHAR, 20, 0, null);
        if (arg.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
