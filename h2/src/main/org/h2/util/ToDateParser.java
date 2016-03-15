/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Daniel Gredler
 */
package org.h2.util;

import static java.lang.String.format;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * This class holds and handles the input data form the TO_DATE-method
 */
public class ToDateParser {
    private final String unmodifiedInputStr;
    private final String unmodifiedFormatStr;
    private final ConfigParam functionName;
    private String inputStr;
    private String formatStr;
    private final Calendar resultCalendar = (Calendar) Calendar.getInstance().clone();
    private Integer nanos;

    /**
     * @param input the input date with the date-time info
     * @param format the format of date-time info
     * @param functionName one of [TO_DATE, TO_TIMESTAMP] (both share the same
     *            code)
     */
    private ToDateParser(ConfigParam functionName, String input, String format) {
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
        // Keep a copy
        unmodifiedInputStr = inputStr;
        if (format == null || format.isEmpty()) {
            // default Oracle format.
            formatStr = functionName.getDefaultFormatStr();
        } else {
            formatStr = format.trim();
        }
        // Keep a copy
        unmodifiedFormatStr = formatStr;
    }

    private static ToDateParser getDateParser(String input, String format) {
        ToDateParser result = new ToDateParser(ConfigParam.TO_DATE, input, format);
        parse(result);
        return result;
    }

    private static ToDateParser getTimestampParser(String input, String format) {
        ToDateParser result = new ToDateParser(ConfigParam.TO_TIMESTAMP, input, format);
        parse(result);
        return result;
    }

    private Timestamp getResultingTimestamp() {
        Calendar cal = (Calendar) getResultCalendar().clone();
        int nanosToSet = nanos == null ?
                cal.get(Calendar.MILLISECOND) * 1000000 : nanos.intValue();
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

    String getFunctionName() {
        return functionName.name();
    }

    void setNanos(int nanos) {
        this.nanos = nanos;
    }

    private boolean hasToParseData() {
        return formatStr.length() > 0;
    }

    private void removeFirstChar() {
        if (!formatStr.isEmpty()) {
            formatStr = formatStr.substring(1);
        }
        if (!inputStr.isEmpty()) {
            inputStr = inputStr.substring(1);
        }
    }

    private static ToDateParser parse(ToDateParser p) {
        while (p.hasToParseData()) {
            List<ToDateTokenizer.FormatTokenEnum> tokenList =
                    ToDateTokenizer.FormatTokenEnum.getTokensInQuestion(p.getFormatStr());
            if (tokenList.isEmpty()) {
                p.removeFirstChar();
                continue;
            }
            boolean foundAnToken = false;
            for (ToDateTokenizer.FormatTokenEnum token : tokenList) {
                if (token.parseFormatStrWithToken(p)) {
                    foundAnToken = true;
                    break;
                }
            }
            if (!foundAnToken) {
                p.removeFirstChar();
                continue;
            }
        }
        return p;
    }

    /**
     * Remove a token from a string.
     *
     * @param inputFragmentStr the input fragment
     * @param formatFragment the format fragment
     */
    void remove(String inputFragmentStr, String formatFragment) {
        if (inputFragmentStr != null && inputStr.length() >= inputFragmentStr.length()) {
            inputStr = inputStr.substring(inputFragmentStr.length());
        }
        if (formatFragment != null && formatStr.length() >= formatFragment.length()) {
            formatStr = formatStr.substring(formatFragment.length());
        }
    }

    @Override
    public String toString() {
        int inputStrLen = inputStr.length();
        int orgInputLen = unmodifiedInputStr.length();
        int currentInputPos = orgInputLen - inputStrLen;
        int restInputLen = inputStrLen <= 0 ? inputStrLen : inputStrLen - 1;

        int orgFormatLen = unmodifiedFormatStr.length();
        int currentFormatPos = orgFormatLen - formatStr.length();

        StringBuilder sb = new StringBuilder();
        sb.append(format("\n    %s('%s', '%s')", functionName,
                unmodifiedInputStr, unmodifiedFormatStr));
        sb.append(format("\n      %s^%s ,  %s^ <-- Parsing failed at this point",
                format("%" + (functionName.name().length() + currentInputPos) + "s", ""),
                restInputLen <= 0 ? "" : format("%" + restInputLen + "s", ""),
                currentFormatPos <= 0 ? "" : format("%" + currentFormatPos + "s", "")));

        return sb.toString();
    }

    /**
     * Parse a string as a timestamp with the given format.
     *
     * @param input the input
     * @param format the format
     * @return the timestamp
     */
    public static Timestamp toTimestamp(String input, String format) {
        ToDateParser parser = getTimestampParser(input, format);
        return parser.getResultingTimestamp();
    }

    /**
     * Parse a string as a date with the given format.
     *
     * @param input the input
     * @param format the format
     * @return the date as a timestamp
     */
    public static Timestamp toDate(String input, String format) {
        ToDateParser parser = getDateParser(input, format);
        return parser.getResultingTimestamp();
    }

    /**
     * The configuration of the date parser.
     */
    private enum ConfigParam {
        TO_DATE("DD MON YYYY"),
        TO_TIMESTAMP("DD MON YYYY HH:MI:SS");

        private final String defaultFormatStr;
        ConfigParam(String defaultFormatStr) {
            this.defaultFormatStr = defaultFormatStr;
        }
        String getDefaultFormatStr() {
            return defaultFormatStr;
        }

    }

}