package org.h2.util;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
 * Emulates Oracle's TO_DATE function.<br>
 * This class knows all about the TO_DATE-format conventions and how to parse the corresponding data
 */
class ToDateTokenizer {
    private static final Pattern PATTERN_Number = Pattern.compile("^([+-]?[0-9]*)");
    private static final Pattern PATTERN_4_Digit = Pattern.compile("^([+-]?[0-9]{4})");
    private static final Pattern PATTERN_3_Digit = Pattern.compile("^([+-]?[0-9]{3})");
    private static final Pattern PATTERN_2_Digit = Pattern.compile("^([+-]?[0-9]{2})");
    private static final Pattern PATTERN_2_DigitOrLess = Pattern.compile("^([+-]?[0-9][0-9]?)");
    private static final Pattern PATTERN_1_Digit = Pattern.compile("^([+-]?[0-9])");
    private static final Pattern PATTERN_AM = Pattern.compile("^(AM|A\\.M\\.)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_PM = Pattern.compile("^(PM|P\\.M\\.)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_AD = Pattern.compile("^(AD|A\\.D\\.)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_BC = Pattern.compile("^(BC|B\\.C\\.)", Pattern.CASE_INSENSITIVE);
    private static final YearParslet PARSLET_Year = new YearParslet();
    private static final MonthParslet PARSLET_Month = new MonthParslet();
    private static final WeekParslet PARSLET_Week = new WeekParslet();
    private static final DayParslet PARSLET_Day = new DayParslet();
    private static final TimeParslet PARSLET_Time = new TimeParslet();

    static enum FormatTokenEnum {
        YYYY(PARSLET_Year) // 4-digit year
        , SYYYY(PARSLET_Year) // 4-digit year with sign (- = B.C.)
        , IYYY(PARSLET_Year) // 4-digit year based on the ISO standard (?)
        , YYY(PARSLET_Year) //
        , IYY(PARSLET_Year) //
        , YY(PARSLET_Year) //
        , IY(PARSLET_Year) //
        , SCC(PARSLET_Year) // Two-digit century with with sign (- = B.C.)
        , CC(PARSLET_Year) // Two-digit century.
        , RRRR(PARSLET_Year) // 2-digit -> 4-digit year 0-49 -> 20xx , 50-99 -> 19xx
        , RR(PARSLET_Year) // last 2-digit of the year using "current" century value.
        , AD(PARSLET_Year, PATTERN_AD) // Meridian indicator
        , BC(PARSLET_Year, PATTERN_BC) // Meridian indicator
        , MONTH(PARSLET_Month) // Full Name of month
        , MON(PARSLET_Month) // Abbreviated name of month.
        , MM(PARSLET_Month) // Month (01-12; JAN = 01).
        , RM(PARSLET_Month) // Roman numeral month (I-XII; JAN = I).
        , WW(PARSLET_Week) // Week of year (1-53)
        , IW(PARSLET_Week) // Week of year (1-52 or 1-53) based on the ISO standard.
        , DDD(PARSLET_Day) // Day of year (1-366).
        , DAY(PARSLET_Day) // Name of day.
        , DD(PARSLET_Day) // Day of month (1-31).
        , DY(PARSLET_Day) // Abbreviated name of day.
        , HH24(PARSLET_Time) //
        , HH12(PARSLET_Time) //
        , HH(PARSLET_Time) // Hour of day (1-12).
        , MI(PARSLET_Time) // Min
        , SSSSS(PARSLET_Time) // Seconds past midnight
        , SS(PARSLET_Time) //
        , TZH(PARSLET_Time) // Time zone hour.
        , TZM(PARSLET_Time) // Time zone minute.
        , TZR(PARSLET_Time) // Time zone region ID
        , AM(PARSLET_Time, PATTERN_AM) // Meridian indicator
        , PM(PARSLET_Time, PATTERN_PM) // Meridian indicator
        // 1 char
        , Y(PARSLET_Year) //
        , I(PARSLET_Year) //
        , Q(PARSLET_Month) // Quarter of year (1, 2, 3, 4; JAN-MAR = 1).
        , W(PARSLET_Week) // Week of month (1-5)
        , D(PARSLET_Day) // Day of week (1-7).
        , J(PARSLET_Day) // Julian day; the number of days since January 1, 4712 BC.
        ;

        private final static Map<Character, List<FormatTokenEnum>> cache = new HashMap<Character, List<FormatTokenEnum>>(FormatTokenEnum.values().length);
        private final ToDateParslet toDateParslet;
        private final Pattern patternToUse;

        FormatTokenEnum(final ToDateParslet toDateParslet, final Pattern patternToUse) {
            this.toDateParslet = toDateParslet;
            this.patternToUse = patternToUse;
        }

        FormatTokenEnum(final ToDateParslet toDateParslet) {
            this.toDateParslet = toDateParslet;
            patternToUse = Pattern.compile(format("^(%s)", name()), Pattern.CASE_INSENSITIVE);
        }

        private static List<FormatTokenEnum> EMPTY_LIST = new ArrayList<FormatTokenEnum>(0);

        /**
         * OPTIMISATION: Only return a list of {@link FormatTokenEnum} that share the same 1st char
         * using the 1st char of the 'to parse' formatStr. Or return empty list if no match.
         */
        static List<FormatTokenEnum> getTokensInQuestion(final ToDateParams params) {
            List<FormatTokenEnum> result = EMPTY_LIST;
            if (cache.size() <= 0) {
                initCache();
            }
            String formatStr = params.getFormatStr();
            if (formatStr != null && formatStr.length() > 0) {
                Character key = Character.toUpperCase(formatStr.charAt(0));
                result = cache.get(key);
            }
            if (result == null) {
                result = EMPTY_LIST;
            }
            return result;
        }

        private static synchronized void initCache() {
            if (cache.size() <= 0) {
                for (FormatTokenEnum token : FormatTokenEnum.values()) {
                    Character tokenKey = Character.toUpperCase(token.name().charAt(0));
                    List<FormatTokenEnum> l = cache.get(tokenKey);
                    if (l == null) {
                        l = new ArrayList<FormatTokenEnum>(1);
                        cache.put(tokenKey, l);
                    }
                    l.add(token);
                }
            }

        }

        /**
         * Parse the format-string with passed token of {@link FormatTokenEnum}}.<br>
         * if token matches return true otherwise false.
         */
        boolean parseFormatStrWithToken(final ToDateParams params) {
            Matcher matcher = patternToUse.matcher(params.getFormatStr());
            boolean foundToken = matcher.find();
            if (foundToken) {
                String formatTokenStr = matcher.group(1);
                toDateParslet.parse(params, this, formatTokenStr);
            }
            return foundToken;
        }
    }

    /**
     * Interface of the classes that can parse a specialized small bit of the TO_DATE format-string
     */
    interface ToDateParslet {
        ToDateParams parse(ToDateParams params, FormatTokenEnum formatTokenEnum, String formatTokenStr);
    }

    /**
     *
     */
    private static final class YearParslet implements ToDateParslet {
        @Override
        public ToDateParams parse(final ToDateParams params, final FormatTokenEnum formatTokenEnum,
                final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String intputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case SYYYY:
            case YYYY:
            case IYYY:
                intputFragmentStr = matchStringOrDie(PATTERN_4_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case YYY:
            case IYY:
                intputFragmentStr = matchStringOrDie(PATTERN_3_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case RRRR:
                intputFragmentStr = matchStringOrDie(PATTERN_2_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                dateNr += dateNr < 50 ? 2000 : 1900;
                result.set(Calendar.YEAR, dateNr);
                break;
            case RR:
                Calendar calendar = Calendar.getInstance();
                int cc = (calendar.get(Calendar.YEAR) / 100);
                intputFragmentStr = matchStringOrDie(PATTERN_2_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr) + cc * 100;
                result.set(Calendar.YEAR, dateNr);
                break;
            case YY:
            case IY:
                intputFragmentStr = matchStringOrDie(PATTERN_2_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case SCC:
            case CC:
                intputFragmentStr = matchStringOrDie(PATTERN_2_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr) * 100;
                result.set(Calendar.YEAR, dateNr);
                break;
            case Y:
            case I:
                intputFragmentStr = matchStringOrDie(PATTERN_1_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case AD:
                intputFragmentStr = matchStringOrDie(PATTERN_AD, params, formatTokenEnum);
                result.set(Calendar.ERA, GregorianCalendar.AD);
                break;
            case BC:
                intputFragmentStr = matchStringOrDie(PATTERN_BC, params, formatTokenEnum);
                result.set(Calendar.ERA, GregorianCalendar.BC);
                break;
            default:
                throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                        .getSimpleName(), formatTokenEnum));
            }
            params.remove(intputFragmentStr, formatTokenStr);
            return params;
        }
    }

    /**
     *
     */
    private static final class MonthParslet implements ToDateParslet {
        private static String[] ROMAN_Month = { "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI",
        "XII" };

        @Override
        public ToDateParams parse(final ToDateParams params, final FormatTokenEnum formatTokenEnum,
                final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            final String s = params.getInputStr();
            String intputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case MONTH:
                intputFragmentStr = setByName(result, params, Calendar.MONTH, Calendar.LONG);
                break;
            case Q:
                throwException(
                        params,
                        format("token '%s' not implemented jet. Donate to H2 and ask for an update :-)",
                                formatTokenEnum.name()));
                break;
            case MON:
                intputFragmentStr = setByName(result, params, Calendar.MONTH, Calendar.SHORT);
                break;
            case MM:
                // Note: In Calendar Month go from 0 - 11 ! :-(
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.MONTH, dateNr - 1);
                break;
            case RM:
                dateNr = 0;
                for (String monthName : ROMAN_Month) {
                    dateNr++;
                    int leng = monthName.length();
                    if (s.length() >= leng && monthName.equalsIgnoreCase(s.substring(0, leng))) {
                        result.set(Calendar.MONTH, dateNr);
                        intputFragmentStr = monthName;
                        break;
                    }
                }
                if (intputFragmentStr == null || intputFragmentStr.isEmpty()) {
                    throwException(params,
                            format("Issue happend when parsing token '%s'. Expected one of: %s",
                                    formatTokenEnum.name(), Arrays.toString(ROMAN_Month)));
                }
                break;
            default:
                throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                        .getSimpleName(), formatTokenEnum));
            }
            params.remove(intputFragmentStr, formatTokenStr);
            return params;
        }
    }

    /**
     *
     */
    private static final class WeekParslet implements ToDateParslet {
        @Override
        public ToDateParams parse(final ToDateParams params, final FormatTokenEnum formatTokenEnum,
                final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String intputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case WW:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                // The first week of the month, as defined by
                // getFirstDayOfWeek() and getMinimalDaysInFirstWeek(),
                result.set(Calendar.WEEK_OF_YEAR, dateNr);
                break;
            case IW:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                /*
                 * Build set the calendar to ISO8601 (see
                 * http://en.wikipedia.org/wiki/ISO_8601_week_number)
                 */
                result.setMinimalDaysInFirstWeek(4);
                result.setFirstDayOfWeek(Calendar.MONDAY);
                result.set(Calendar.WEEK_OF_YEAR, dateNr);
                break;
            case W:
                intputFragmentStr = matchStringOrDie(PATTERN_1_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                // The first week of the month, as defined by
                // getFirstDayOfWeek() and getMinimalDaysInFirstWeek(),
                result.set(Calendar.WEEK_OF_MONTH, dateNr);
                break;
            default:
                throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                        .getSimpleName(), formatTokenEnum));
            }
            params.remove(intputFragmentStr, formatTokenStr);
            return params;
        }
    }

    /**
     *
     */
    private static final class DayParslet implements ToDateParslet {
        @Override
        public ToDateParams parse(final ToDateParams params, final FormatTokenEnum formatTokenEnum,
                final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String intputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case DDD:
                intputFragmentStr = matchStringOrDie(PATTERN_Number, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.DAY_OF_YEAR, dateNr);
                break;
            case DD:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.DAY_OF_MONTH, dateNr);
                break;
            case D:
                intputFragmentStr = matchStringOrDie(PATTERN_1_Digit, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.DAY_OF_MONTH, dateNr);
                break;
            case DAY:
                intputFragmentStr = setByName(result, params, Calendar.DAY_OF_WEEK, Calendar.LONG);
                break;
            case DY:
                intputFragmentStr = setByName(result, params, Calendar.DAY_OF_WEEK, Calendar.SHORT);
                break;
            case J:
                throwException(
                        params,
                        format("token '%s' not implemented jet. Donate to H2 and ask for an update :-)",
                                formatTokenEnum.name()));
                break;
            default:
                throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                        .getSimpleName(), formatTokenEnum));
            }
            params.remove(intputFragmentStr, formatTokenStr);
            return params;
        }
    }

    private static int MILLIS_in_hour = 60 * 60 * 1000;

    /**
     *
     */
    private static final class TimeParslet implements ToDateParslet {

        @Override
        public ToDateParams parse(final ToDateParams params, final FormatTokenEnum formatTokenEnum,
                final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String intputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case HH24:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.HOUR_OF_DAY, dateNr);
                break;
            case HH12:
            case HH:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.HOUR, dateNr);
                break;
            case MI:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.MINUTE, dateNr);
                break;
            case SS:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.SECOND, dateNr);
                break;
            case SSSSS:
                intputFragmentStr = matchStringOrDie(PATTERN_Number, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, dateNr);
                break;
            case AM:
                intputFragmentStr = matchStringOrDie(PATTERN_AM, params, formatTokenEnum);
                result.set(Calendar.AM_PM, Calendar.AM);
                break;
            case PM:
                intputFragmentStr = matchStringOrDie(PATTERN_PM, params, formatTokenEnum);
                result.set(Calendar.AM_PM, Calendar.PM);
                break;
            case TZH:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                TimeZone tz = result.getTimeZone();
                int offsetMillis = tz.getRawOffset();
                offsetMillis = (offsetMillis / MILLIS_in_hour) * MILLIS_in_hour; // purge min and sec
                tz.setRawOffset(offsetMillis + dateNr);
                result.setTimeZone(tz);
                break;
            case TZM:
                intputFragmentStr = matchStringOrDie(PATTERN_2_DigitOrLess, params, formatTokenEnum);
                dateNr = parseInt(intputFragmentStr);
                tz = result.getTimeZone();
                offsetMillis = tz.getRawOffset();
                offsetMillis = offsetMillis % MILLIS_in_hour; // purge hour
                tz.setRawOffset(dateNr * MILLIS_in_hour + offsetMillis);
                result.setTimeZone(tz);
                break;
            case TZR:
                final String s = params.getInputStr();
                tz = result.getTimeZone();
                for (String tzName : TimeZone.getAvailableIDs()) {
                    int leng = tzName.length();
                    if (s.length() >= leng && tzName.equalsIgnoreCase(s.substring(0, leng))) {
                        tz.setID(tzName);
                        result.setTimeZone(tz);
                        intputFragmentStr = tzName;
                        break;
                    }
                }
                break;

            default:
                throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                        .getSimpleName(), formatTokenEnum));
            }
            params.remove(intputFragmentStr, formatTokenStr);
            return params;
        }
    }

    // ========== PRIVATE ===================

    private static int parseInt(final String s) {
        int result = 0;
        if (s.length() > 0 && s.charAt(0) == '+') {
            result = Integer.parseInt(s.substring(1));
        } else {
            result = Integer.parseInt(s);
        }
        return result;
    }

    private static String matchStringOrDie(final Pattern p, final ToDateParams params, final Enum<?> aEnum) {
        final String s = params.getInputStr();
        Matcher matcher = p.matcher(s);
        if (!matcher.find()) {
            throwException(params, format("Issue happend when parsing token '%s'", aEnum.name()));
        }
        return matcher.group(1);
    }

    private static String setByName(final Calendar c, final ToDateParams params, final int field, final int style) {
        String intputFragmentStr = null;
        String s = params.getInputStr();
        Map<String, Integer> timeStringMap = c.getDisplayNames(field, style, Locale.getDefault());
        for (String dayName : timeStringMap.keySet()) {
            int leng = dayName.length();
            if (dayName.equalsIgnoreCase(s.substring(0, leng))) {
                c.set(field, timeStringMap.get(dayName));
                intputFragmentStr = dayName;
                break;
            }
        }
        if (intputFragmentStr == null || intputFragmentStr.isEmpty()) {
            throwException(params, format("Tryed to parse one of '%s' but failed (may be an internal error?)",
                    timeStringMap.keySet()));
        }
        return intputFragmentStr;
    }

    private static void throwException(final ToDateParams params, final String errorStr) {
        throw DbException.get(
                ErrorCode.INVALID_TO_DATE_FORMAT,
                params.getFunctionName().name(),
                format(" %s. Details: %s", errorStr, params));
    }

}
