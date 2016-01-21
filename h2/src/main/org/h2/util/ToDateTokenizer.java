/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Daniel Gredler
 */
package org.h2.util;

import static java.lang.String.format;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Emulates Oracle's TO_DATE function. This class knows all about the
 * TO_DATE-format conventions and how to parse the corresponding data.
 */
class ToDateTokenizer {

    /**
     * The pattern for a number.
     */
    static final Pattern PATTERN_NUMBER = Pattern.compile("^([+-]?[0-9]+)");

    /**
     * The pattern for for digits (typically a year).
     */
    static final Pattern PATTERN_FOUR_DIGITS = Pattern.compile("^([+-]?[0-9]{4})");

    /**
     * The pattern for three digits.
     */
    static final Pattern PATTERN_THREE_DIGITS = Pattern.compile("^([+-]?[0-9]{3})");

    /**
     * The pattern for two digits.
     */
    static final Pattern PATTERN_TWO_DIGITS = Pattern.compile("^([+-]?[0-9]{2})");

    /**
     * The pattern for one or two digits.
     */
    static final Pattern PATTERN_TWO_DIGITS_OR_LESS =
            Pattern.compile("^([+-]?[0-9][0-9]?)");

    /**
     * The pattern for one digit.
     */
    static final Pattern PATTERN_ONE_DIGIT =
            Pattern.compile("^([+-]?[0-9])");

    /**
     * The pattern for a fraction (of a second for example).
     */
    static final Pattern PATTERN_FF =
            Pattern.compile("^(FF[0-9]?)", Pattern.CASE_INSENSITIVE);

    /**
     * The pattern for "am" or "pm".
     */
    static final Pattern PATTERN_AM_PM =
            Pattern.compile("^(AM|A\\.M\\.|PM|P\\.M\\.)", Pattern.CASE_INSENSITIVE);

    /**
     * The pattern for "bc" or "ad".
     */
    static final Pattern PATTERN_BC_AD =
            Pattern.compile("^(BC|B\\.C\\.|AD|A\\.D\\.)", Pattern.CASE_INSENSITIVE);

    /**
     * The parslet for a year.
     */
    static final YearParslet PARSLET_YEAR = new YearParslet();

    /**
     * The parslet for a month.
     */
    static final MonthParslet PARSLET_MONTH = new MonthParslet();

    /**
     * The parslet for a day.
     */
    static final DayParslet PARSLET_DAY = new DayParslet();

    /**
     * The parslet for time.
     */
    static final TimeParslet PARSLET_TIME = new TimeParslet();

    /**
     * The number of milliseconds in a day.
     */
    static final int MILLIS_IN_HOUR = 60 * 60 * 1000;

    /**
     * Interface of the classes that can parse a specialized small bit of the
     * TO_DATE format-string.
     */
    interface ToDateParslet {

        /**
         * Parse a date part.
         *
         * @param params the parameters that contains the string
         * @param formatTokenEnum the format
         * @param formatTokenStr the format string
         */
        void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr);
    }

    /**
     * Parslet responsible for parsing year parameter
     */
    static class YearParslet implements ToDateParslet {
        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            Calendar result = params.getResultCalendar();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case SYYYY:
                case YYYY:
                case IYYY:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_FOUR_DIGITS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0.
                    // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case YYY:
                case IYY:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_THREE_DIGITS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0.
                    // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case RRRR:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    dateNr += dateNr < 50 ? 2000 : 1900;
                    result.set(Calendar.YEAR, dateNr);
                    break;
                case RR:
                    Calendar calendar = Calendar.getInstance();
                    int cc = calendar.get(Calendar.YEAR) / 100;
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr) + cc * 100;
                    result.set(Calendar.YEAR, dateNr);
                    break;
                case EE /*NOT supported yet*/:
                    throwException(params, format(
                            "token '%s' not supported yet.", formatTokenEnum.name()));
                    break;
                case E /*NOT supported yet*/:
                    throwException(params, format(
                            "token '%s' not supported yet.", formatTokenEnum.name()));
                    break;
                case YY:
                case IY:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0.
                    // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case SCC:
                case CC:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr) * 100;
                    result.set(Calendar.YEAR, dateNr);
                    break;
                case Y:
                case I:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_ONE_DIGIT, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0.
                    // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case BC_AD:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_BC_AD, params, formatTokenEnum);
                    if (inputFragmentStr.toUpperCase().startsWith("B")) {
                        result.set(Calendar.ERA, GregorianCalendar.BC);
                    } else {
                        result.set(Calendar.ERA, GregorianCalendar.AD);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(format(
                            "%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing month parameter
     */
    static class MonthParslet implements ToDateParslet {
        private static final String[] ROMAN_MONTH = { "I", "II", "III", "IV",
                "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII" };

        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            Calendar result = params.getResultCalendar();
            String s = params.getInputStr();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case MONTH:
                    inputFragmentStr = setByName(result, params,
                            Calendar.MONTH, Calendar.LONG);
                    break;
                case Q /*NOT supported yet*/:
                    throwException(params, format(
                            "token '%s' not supported yet.", formatTokenEnum.name()));
                    break;
                case MON:
                    inputFragmentStr = setByName(result, params,
                            Calendar.MONTH, Calendar.SHORT);
                    break;
                case MM:
                    // Note: In Calendar Month go from 0 - 11
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.MONTH, dateNr - 1);
                    break;
                case RM:
                    dateNr = 0;
                    for (String monthName : ROMAN_MONTH) {
                        dateNr++;
                        int len = monthName.length();
                        if (s.length() >= len &&
                                monthName.equalsIgnoreCase(s.substring(0, len))) {
                            result.set(Calendar.MONTH, dateNr);
                            inputFragmentStr = monthName;
                            break;
                        }
                    }
                    if (inputFragmentStr == null || inputFragmentStr.isEmpty()) {
                        throwException(params,
                                format("Issue happened when parsing token '%s'. " +
                                        "Expected one of: %s",
                                        formatTokenEnum.name(), Arrays.toString(ROMAN_MONTH)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(format(
                            "%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing day parameter
     */
    static class DayParslet implements ToDateParslet {
        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            Calendar result = params.getResultCalendar();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case DDD:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_NUMBER, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.DAY_OF_YEAR, dateNr);
                    break;
                case DD:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.DAY_OF_MONTH, dateNr);
                    break;
                case D:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_ONE_DIGIT, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.DAY_OF_MONTH, dateNr);
                    break;
                case DAY:
                    inputFragmentStr = setByName(result, params,
                            Calendar.DAY_OF_WEEK, Calendar.LONG);
                    break;
                case DY:
                    inputFragmentStr = setByName(result, params,
                            Calendar.DAY_OF_WEEK, Calendar.SHORT);
                    break;
                case J:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_NUMBER, params, formatTokenEnum);
                    try {
                        Date date = new SimpleDateFormat("Myydd").parse(inputFragmentStr);
                        result.setTime(date);
                    } catch (ParseException e) {
                        throwException(params, format(
                                "Failed to parse Julian date: %s", inputFragmentStr));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(format(
                            "%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing time parameter
     */
    static class TimeParslet implements ToDateParslet {

        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            Calendar result = params.getResultCalendar();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case HH24:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.HOUR_OF_DAY, dateNr);
                    break;
                case HH12:
                case HH:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.HOUR, dateNr);
                    break;
                case MI:
                    inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.MINUTE, dateNr);
                    break;
                case SS:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.SECOND, dateNr);
                    break;
                case SSSSS:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_NUMBER, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    result.set(Calendar.HOUR_OF_DAY, 0);
                    result.set(Calendar.MINUTE, 0);
                    result.set(Calendar.SECOND, dateNr);
                    break;
                case FF:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_NUMBER, params, formatTokenEnum);
                    String paddedRightNrStr = format("%-9s", inputFragmentStr).replace(' ', '0');
                    paddedRightNrStr = paddedRightNrStr.substring(0, 9);
                    Double nineDigits = Double.parseDouble(paddedRightNrStr);
                    params.setNanos(nineDigits.intValue());
                    dateNr = (int) Math.round(nineDigits / 1000000.0);
                    result.set(Calendar.MILLISECOND, dateNr);
                    break;
                case AM_PM:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_AM_PM, params, formatTokenEnum);
                    if (inputFragmentStr.toUpperCase().startsWith("A")) {
                        result.set(Calendar.AM_PM, Calendar.AM);
                    } else {
                        result.set(Calendar.AM_PM, Calendar.PM);
                    }
                    break;
                case TZH:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    TimeZone tz = result.getTimeZone();
                    int offsetMillis = tz.getRawOffset();
                    // purge min and sec
                    offsetMillis = (offsetMillis / MILLIS_IN_HOUR) * MILLIS_IN_HOUR;
                    tz.setRawOffset(offsetMillis + dateNr);
                    result.setTimeZone(tz);
                    break;
                case TZM:
                    inputFragmentStr = matchStringOrThrow(
                            PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = Integer.parseInt(inputFragmentStr);
                    tz = result.getTimeZone();
                    offsetMillis = tz.getRawOffset();
                    // purge hour
                    offsetMillis = offsetMillis % MILLIS_IN_HOUR;
                    tz.setRawOffset(dateNr * MILLIS_IN_HOUR + offsetMillis);
                    result.setTimeZone(tz);
                    break;
                case TZR:
                    // Example: US/Pacific
                    String s = params.getInputStr();
                    tz = result.getTimeZone();
                    for (String tzName : TimeZone.getAvailableIDs()) {
                        int length = tzName.length();
                        if (s.length() >= length &&
                                tzName.equalsIgnoreCase(s.substring(0, length))) {
                            tz.setID(tzName);
                            result.setTimeZone(tz);
                            inputFragmentStr = tzName;
                            break;
                        }
                    }
                    break;
                case TZD:
                // Must correspond with TZR region. Example: PST (for US/Pacific
                // standard time)
                    throwException(params, format("token '%s' not supported yet.",
                            formatTokenEnum.name()));
                    break;
                default:
                    throw new IllegalArgumentException(format(
                            "%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Match the pattern, or if not possible throw an exception.
     *
     * @param p the pattern
     * @param params the parameters with the input string
     * @param aEnum the pattern name
     * @return the matched value
     */
    static String matchStringOrThrow(Pattern p, ToDateParser params, Enum<?> aEnum) {
        String s = params.getInputStr();
        Matcher matcher = p.matcher(s);
        if (!matcher.find()) {
            throwException(params, format("Issue happened when parsing token '%s'", aEnum.name()));
        }
        return matcher.group(1);
    }

    /**
     * Set the given field in the calendar.
     *
     * @param c the calendar
     * @param params the parameters with the input string
     * @param field the field to set
     * @param style the data type
     * @return the matched value
     */
    static String setByName(Calendar c, ToDateParser params, int field, int style) {
        String inputFragmentStr = null;
        String s = params.getInputStr();
        Map<String, Integer> timeStringMap = c.getDisplayNames(
                field, style, Locale.getDefault());
        for (String dayName : timeStringMap.keySet()) {
            int len = dayName.length();
            if (dayName.equalsIgnoreCase(s.substring(0, len))) {
                c.set(field, timeStringMap.get(dayName));
                inputFragmentStr = dayName;
                break;
            }
        }
        if (inputFragmentStr == null || inputFragmentStr.isEmpty()) {
            throwException(params, format(
                    "Tried to parse one of '%s' but failed (may be an internal error?)",
                    timeStringMap.keySet()));
        }
        return inputFragmentStr;
    }

    /**
     * Throw a parse exception.
     *
     * @param params the parameters with the input string
     * @param errorStr the error string
     */
    static void throwException(ToDateParser params, String errorStr) {
        throw DbException.get(
                ErrorCode.INVALID_TO_DATE_FORMAT,
                params.getFunctionName(),
                format(" %s. Details: %s", errorStr, params));
    }

    /**
     * The format tokens.
     */
    public static enum FormatTokenEnum {
        // 4-digit year
        YYYY(PARSLET_YEAR),
        // 4-digit year with sign (- = B.C.)
        SYYYY(PARSLET_YEAR),
        // 4-digit year based on the ISO standard (?)
        IYYY(PARSLET_YEAR),
        YYY(PARSLET_YEAR),
        IYY(PARSLET_YEAR),
        YY(PARSLET_YEAR),
        IY(PARSLET_YEAR),
        // Two-digit century with with sign (- = B.C.)
        SCC(PARSLET_YEAR),
        // Two-digit century.
        CC(PARSLET_YEAR),
        // 2-digit -> 4-digit year 0-49 -> 20xx , 50-99 -> 19xx
        RRRR(PARSLET_YEAR),
        // last 2-digit of the year using "current" century value.
        RR(PARSLET_YEAR),
        // Meridian indicator
        BC_AD(PARSLET_YEAR, PATTERN_BC_AD),
        // Full Name of month
        MONTH(PARSLET_MONTH),
        // Abbreviated name of month.
        MON(PARSLET_MONTH),
        // Month (01-12; JAN = 01).
        MM(PARSLET_MONTH),
        // Roman numeral month (I-XII; JAN = I).
        RM(PARSLET_MONTH),
        // Day of year (1-366).
        DDD(PARSLET_DAY),
        // Name of day.
        DAY(PARSLET_DAY),
        // Day of month (1-31).
        DD(PARSLET_DAY),
        // Abbreviated name of day.
        DY(PARSLET_DAY),
        HH24(PARSLET_TIME),
        HH12(PARSLET_TIME),
        // Hour of day (1-12).
        HH(PARSLET_TIME),
        // Min
        MI(PARSLET_TIME),
        // Seconds past midnight (0-86399)
        SSSSS(PARSLET_TIME),
        SS(PARSLET_TIME),
        // Fractional seconds
        FF(PARSLET_TIME, PATTERN_FF),
        // Time zone hour.
        TZH(PARSLET_TIME),
        // Time zone minute.
        TZM(PARSLET_TIME),
        // Time zone region ID
        TZR(PARSLET_TIME),
        // Daylight savings information. Example:
        // PST (for US/Pacific standard time);
        TZD(PARSLET_TIME),
        // Meridian indicator
        AM_PM(PARSLET_TIME, PATTERN_AM_PM),
        // NOT supported yet -
        // Full era name (Japanese Imperial, ROC Official,
        // and Thai Buddha calendars).
        EE(PARSLET_YEAR),
        // NOT supported yet -
        // Abbreviated era name (Japanese Imperial,
        // ROC Official, and Thai Buddha calendars).
        E(PARSLET_YEAR),
        Y(PARSLET_YEAR),
        I(PARSLET_YEAR),
        // Quarter of year (1, 2, 3, 4; JAN-MAR = 1).
        Q(PARSLET_MONTH),
        // Day of week (1-7).
        D(PARSLET_DAY),
        // NOT supported yet -
        // Julian day; the number of days since Jan 1, 4712 BC.
        J(PARSLET_DAY);

        private static final List<FormatTokenEnum> EMPTY_LIST =
                new ArrayList<FormatTokenEnum>(0);

        private static final Map<Character, List<FormatTokenEnum>> CACHE =
                new HashMap<Character, List<FormatTokenEnum>>(FormatTokenEnum.values().length);
        private final ToDateParslet toDateParslet;
        private final Pattern patternToUse;

        /**
         * Construct a format token.
         *
         * @param toDateParslet the date parslet
         * @param patternToUse the pattern
         */
        FormatTokenEnum(ToDateParslet toDateParslet, Pattern patternToUse) {
            this.toDateParslet = toDateParslet;
            this.patternToUse = patternToUse;
        }

        /**
         * Construct a format token.
         *
         * @param toDateParslet the date parslet
         */
        FormatTokenEnum(ToDateParslet toDateParslet) {
            this.toDateParslet = toDateParslet;
            patternToUse = Pattern.compile(format("^(%s)", name()), Pattern.CASE_INSENSITIVE);
        }

        /**
         * Optimization: Only return a list of {@link FormatTokenEnum} that
         * share the same 1st char using the 1st char of the 'to parse'
         * formatStr. Or return empty list if no match.
         *
         * @param formatStr the format string
         * @return the list of tokens
         */
        static List<FormatTokenEnum> getTokensInQuestion(String formatStr) {
            List<FormatTokenEnum> result = EMPTY_LIST;
            if (CACHE.size() <= 0) {
                initCache();
            }
            if (formatStr != null && formatStr.length() > 0) {
                Character key = Character.toUpperCase(formatStr.charAt(0));
                result = CACHE.get(key);
            }
            if (result == null) {
                result = EMPTY_LIST;
            }
            return result;
        }

        private static synchronized void initCache() {
            if (CACHE.size() <= 0) {
                for (FormatTokenEnum token : FormatTokenEnum.values()) {

                    List<Character> tokenKeys = new ArrayList<Character>();

                    if (token.name().contains("_")) {
                        String[] tokens = token.name().split("_");
                        for (String tokenLets : tokens) {
                            tokenKeys.add(tokenLets.toUpperCase().charAt(0));
                        }
                    } else {
                        tokenKeys.add(token.name().toUpperCase().charAt(0));
                    }

                    for (Character tokenKey : tokenKeys) {
                        List<FormatTokenEnum> l = CACHE.get(tokenKey);
                        if (l == null) {
                            l = new ArrayList<FormatTokenEnum>(1);
                            CACHE.put(tokenKey, l);
                        }
                        l.add(token);
                    }
                }
            }

        }

        /**
         * Parse the format-string with passed token of {@link FormatTokenEnum}.
         * If token matches return true, otherwise false.
         *
         * @param params the parameters
         * @return true if it matches
         */
        boolean parseFormatStrWithToken(ToDateParser params) {
            Matcher matcher = patternToUse.matcher(params.getFormatStr());
            boolean foundToken = matcher.find();
            if (foundToken) {
                String formatTokenStr = matcher.group(1);
                toDateParslet.parse(params, this, formatTokenStr);
            }
            return foundToken;
        }
    }

}