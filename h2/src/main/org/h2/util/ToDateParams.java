package org.h2.util;

import static java.lang.String.format;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

import org.h2.util.ToDate.ToDateFunctionName;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * This class holds and handles the input data form the TO_DATE-method
 */
class ToDateParams {
    private final String unmodifiedInputStr;
    private final String unmodifiedFormatStr;
    private final ToDateFunctionName functionName;
    private String inputStr;
    private String formatStr;
    private final Calendar resultCalendar = (Calendar) Calendar.getInstance().clone();
    private Integer nanos = null;

    /**
     * @param input the input date with the date-time info
     * @param format  the format of date-time info
     * @param functionName one of [TO_DATE, TO_TIMESTAMP] (both share the same code)
     */
    ToDateParams(final ToDateFunctionName functionName, final String input, final String format) {
        // reset calendar - default oracle behaviour
        resultCalendar.set(Calendar.YEAR, 1970);
        resultCalendar.set(Calendar.MONTH, Calendar.getInstance().get(Calendar.MONTH));
        resultCalendar.clear(Calendar.DAY_OF_YEAR);
        resultCalendar.clear(Calendar.DAY_OF_WEEK);
        resultCalendar.clear(Calendar.DAY_OF_WEEK_IN_MONTH);
        resultCalendar.set(Calendar.DAY_OF_MONTH, 1);
        resultCalendar.set(Calendar.HOUR, 0);
        resultCalendar.set(Calendar.HOUR_OF_DAY, 0);
        resultCalendar.set(Calendar.MINUTE, 0);
        resultCalendar.set(Calendar.SECOND, 0);
        resultCalendar.set(Calendar.MILLISECOND, 0);
        resultCalendar.set(Calendar.AM_PM, Calendar.AM);

        this.functionName = functionName;
        inputStr = input.trim();
        unmodifiedInputStr = inputStr; // Keep a copy
        if (format == null || format.isEmpty()) {
            formatStr = functionName.getDefaultFormatStr(); // default Oracle format.
        } else {
            formatStr = format.trim();
        }
        unmodifiedFormatStr = formatStr; // Keep a copy
    }

    Date getResultingDate() {
        return new Date(getResultCalendar().getTimeInMillis());
    }

    Timestamp getResultingTimestamp() {
        Calendar cal = (Calendar) getResultCalendar().clone();
        int nanosToSet = nanos == null ? cal.get(Calendar.MILLISECOND) * 1000000 : nanos.intValue();
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(nanosToSet);
        return ts;
    }

    Calendar getResultCalendar() {
        return resultCalendar;
    }

    String getInputStr() {
        return inputStr;
    }

    String getFormatStr() {
        return formatStr;
    }

    ToDateFunctionName getFunctionName() {
        return functionName;
    }

    void setNanos(final int nanos) {
        this.nanos = nanos;
    }

    boolean hasToParseData() {
        return formatStr.length() > 0;
    }

    void removeFirstChar() {
        if (!formatStr.isEmpty()) {
            formatStr = formatStr.substring(1);
        }
        if (!inputStr.isEmpty()) {
            inputStr = inputStr.substring(1);
        }
    }

    void remove(final String toIgnore) {
        if (toIgnore != null) {
            int trimLeng = toIgnore.length();
            formatStr = formatStr.substring(trimLeng);
            if (inputStr.length() >= trimLeng) {
                inputStr = inputStr.substring(trimLeng);
            }
        }
    }

    void remove(final String intputFragmentStr, final String formatFragment) {
        if (intputFragmentStr != null && inputStr.length() >= intputFragmentStr.length()) {
            inputStr = inputStr.substring(intputFragmentStr.length());
        }
        if (formatFragment != null && formatStr.length() >= formatFragment.length()) {
            formatStr = formatStr.substring(formatFragment.length());
        }
    }

    @Override
    public String toString() {
        int inputStrLeng = inputStr.length();
        int orgInputLeng = unmodifiedInputStr.length();
        int currentInputPos = orgInputLeng - inputStrLeng;
        int restInputLeng = inputStrLeng <= 0 ? inputStrLeng : inputStrLeng - 1;

        int orgFormatLeng = unmodifiedFormatStr.length();
        int currentFormatPos = orgFormatLeng - formatStr.length();

        StringBuilder sb = new StringBuilder();
        sb.append(format("\n    %s('%s', '%s')", functionName, unmodifiedInputStr, unmodifiedFormatStr));
        sb.append(format("\n      %s^%s ,  %s^ <-- Parsing failed at this point", //
                format("%" + (functionName.name().length() + currentInputPos) + "s", ""),
                restInputLeng <= 0 ? "" : format("%" + restInputLeng + "s", ""),
                currentFormatPos <= 0 ? "" : format("%" + currentFormatPos + "s", "")));

        return sb.toString();
    }
}