package org.h2.util;

import static java.lang.String.format;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * This class knows all about the TO_DATE-format conventions and how to parse the corresponding data
 */
class ToDateTokenizer {
    private static final Pattern PATTERN_NUMBER = Pattern.compile("^([+-]?[0-9]+)");
    private static final Pattern PATTERN_FOUR_DIGITS = Pattern.compile("^([+-]?[0-9]{4})");
    private static final Pattern PATTERN_THREE_DIGITS = Pattern.compile("^([+-]?[0-9]{3})");
    private static final Pattern PATTERN_TWO_DIGITS = Pattern.compile("^([+-]?[0-9]{2})");
    private static final Pattern PATTERN_TWO_DIGITS_OR_LESS = Pattern.compile("^([+-]?[0-9][0-9]?)");
    private static final Pattern PATTERN_ONE_DIGIT = Pattern.compile("^([+-]?[0-9])");
    private static final Pattern PATTERN_FF = Pattern.compile("^(FF[0-9]?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_AM_PM = Pattern.compile("^(AM|A\\.M\\.|PM|P\\.M\\.)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_BC_AD = Pattern.compile("^(BC|B\\.C\\.|AD|A\\.D\\.)", Pattern.CASE_INSENSITIVE);
    private static final YearParslet PARSLET_YEAR = new YearParslet();
    private static final MonthParslet PARSLET_MONTH = new MonthParslet();
    private static final DayParslet PARSLET_DAY = new DayParslet();
    private static final TimeParslet PARSLET_TIME = new TimeParslet();

    static enum FormatTokenEnum {
        YYYY(PARSLET_YEAR) // 4-digit year
        , SYYYY(PARSLET_YEAR) // 4-digit year with sign (- = B.C.)
        , IYYY(PARSLET_YEAR) // 4-digit year based on the ISO standard (?)
        , YYY(PARSLET_YEAR) //
        , IYY(PARSLET_YEAR) //
        , YY(PARSLET_YEAR) //
        , IY(PARSLET_YEAR) //
        , SCC(PARSLET_YEAR) // Two-digit century with with sign (- = B.C.)
        , CC(PARSLET_YEAR) // Two-digit century.
        , RRRR(PARSLET_YEAR) // 2-digit -> 4-digit year 0-49 -> 20xx , 50-99 -> 19xx
        , RR(PARSLET_YEAR) // last 2-digit of the year using "current" century value.
        , BC_AD(PARSLET_YEAR, PATTERN_BC_AD) // Meridian indicator
        , MONTH(PARSLET_MONTH) // Full Name of month
        , MON(PARSLET_MONTH) // Abbreviated name of month.
        , MM(PARSLET_MONTH) // Month (01-12; JAN = 01).
        , RM(PARSLET_MONTH) // Roman numeral month (I-XII; JAN = I).
        , DDD(PARSLET_DAY) // Day of year (1-366).
        , DAY(PARSLET_DAY) // Name of day.
        , DD(PARSLET_DAY) // Day of month (1-31).
        , DY(PARSLET_DAY) // Abbreviated name of day.
        , HH24(PARSLET_TIME) //
        , HH12(PARSLET_TIME) //
        , HH(PARSLET_TIME) // Hour of day (1-12).
        , MI(PARSLET_TIME) // Min
        , SSSSS(PARSLET_TIME) // Seconds past midnight (0-86399)
        , SS(PARSLET_TIME) //
        , FF(PARSLET_TIME, PATTERN_FF) // Fractional seconds
        , TZH(PARSLET_TIME) // Time zone hour.
        , TZM(PARSLET_TIME) // Time zone minute.
        , TZR(PARSLET_TIME) // Time zone region ID
        , TZD(PARSLET_TIME) // Daylight savings information. Example: PST (for US/Pacific standard time);
        , AM_PM(PARSLET_TIME, PATTERN_AM_PM) // Meridian indicator
        , EE(PARSLET_YEAR) // NOT supported yet - Full era name (Japanese Imperial, ROC Official, and Thai Buddha calendars).
        , E(PARSLET_YEAR) // NOT supported yet - Abbreviated era name (Japanese Imperial, ROC Official, and Thai Buddha calendars).
        , Y(PARSLET_YEAR) //
        , I(PARSLET_YEAR) //
        , Q(PARSLET_MONTH) // Quarter of year (1, 2, 3, 4; JAN-MAR = 1).
        , D(PARSLET_DAY) // Day of week (1-7).
        , J(PARSLET_DAY); // NOT supported yet - Julian day; the number of days since Jan 1, 4712 BC.


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
        static List<FormatTokenEnum> getTokensInQuestion(String formatStr) {
            List<FormatTokenEnum> result = EMPTY_LIST;
            if (cache.size() <= 0) {
                initCache();
            }
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

                    List<Character> tokenKeys = new ArrayList<Character>();

                    if(token.name().contains("_")) {
                       String[] tokens = token.name().split("_");
                       for(String tokenLets : tokens) {
                           tokenKeys.add(tokenLets.toUpperCase().charAt(0));
                       }
                    } else {
                       tokenKeys.add(token.name().toUpperCase().charAt(0));
                    }

                    for(Character tokenKey : tokenKeys) {
                        List<FormatTokenEnum> l = cache.get(tokenKey);
                        if (l == null) {
                            l = new ArrayList<FormatTokenEnum>(1);
                            cache.put(tokenKey, l);
                        }
                        l.add(token);
                    }
                }
            }

        }

        /**
         * Parse the format-string with passed token of {@link FormatTokenEnum}}.<br>
         * If token matches return true, otherwise false.
         */
        boolean parseFormatStrWithToken(final ToDateParser params) {
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
        void parse(ToDateParser params, FormatTokenEnum formatTokenEnum, String formatTokenStr);
    }

    /**
     * Parslet responsible for parsing year parameter
     */
    private static final class YearParslet implements ToDateParslet {
        @Override
        public void parse(final ToDateParser params, final FormatTokenEnum formatTokenEnum,
                          final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case SYYYY:
                case YYYY:
                case IYYY:
                    inputFragmentStr = matchStringOrDie(PATTERN_FOUR_DIGITS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case YYY:
                case IYY:
                    inputFragmentStr = matchStringOrDie(PATTERN_THREE_DIGITS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case RRRR:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    dateNr += dateNr < 50 ? 2000 : 1900;
                    result.set(Calendar.YEAR, dateNr);
                    break;
                case RR:
                    Calendar calendar = Calendar.getInstance();
                    int cc = (calendar.get(Calendar.YEAR) / 100);
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr) + cc * 100;
                    result.set(Calendar.YEAR, dateNr);
                    break;
                case EE /*NOT supported yet*/:
                    throwException(params, format("token '%s' not supported yet.", formatTokenEnum.name()));
                    break;
                case E /*NOT supported yet*/:
                    throwException(params, format("token '%s' not supported yet.", formatTokenEnum.name()));
                    break;
                case YY:
                case IY:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case SCC:
                case CC:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr) * 100;
                    result.set(Calendar.YEAR, dateNr);
                    break;
                case Y:
                case I:
                    inputFragmentStr = matchStringOrDie(PATTERN_ONE_DIGIT, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    // Gregorian calendar does not have a year 0. 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                    result.set(Calendar.YEAR, dateNr >= 0 ? dateNr : dateNr + 1);
                    break;
                case BC_AD:
                    inputFragmentStr = matchStringOrDie(PATTERN_BC_AD, params, formatTokenEnum);
                    if(inputFragmentStr.toUpperCase().startsWith("B")) {
                        result.set(Calendar.ERA, GregorianCalendar.BC);
                    }
                    else {
                        result.set(Calendar.ERA, GregorianCalendar.AD);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing month parameter
     */
    private static final class MonthParslet implements ToDateParslet {
        private static final String[] ROMAN_Month = { "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
                "XI", "XII" };

        @Override
        public void parse(final ToDateParser params, final FormatTokenEnum formatTokenEnum,
                          final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            final String s = params.getInputStr();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case MONTH:
                    inputFragmentStr = setByName(result, params, Calendar.MONTH, Calendar.LONG);
                    break;
                case Q /*NOT supported yet*/:
                    throwException(params, format("token '%s' not supported jet.", formatTokenEnum.name()));
                    break;
                case MON:
                    inputFragmentStr = setByName(result, params, Calendar.MONTH, Calendar.SHORT);
                    break;
                case MM:
                    // Note: In Calendar Month go from 0 - 11
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.MONTH, dateNr - 1);
                    break;
                case RM:
                    dateNr = 0;
                    for (String monthName : ROMAN_Month) {
                        dateNr++;
                        int leng = monthName.length();
                        if (s.length() >= leng && monthName.equalsIgnoreCase(s.substring(0, leng))) {
                            result.set(Calendar.MONTH, dateNr);
                            inputFragmentStr = monthName;
                            break;
                        }
                    }
                    if (inputFragmentStr == null || inputFragmentStr.isEmpty()) {
                        throwException(params,
                                format("Issue happened when parsing token '%s'. Expected one of: %s",
                                        formatTokenEnum.name(), Arrays.toString(ROMAN_Month)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing day parameter
     */
    private static final class DayParslet implements ToDateParslet {
        @Override
        public void parse(final ToDateParser params, final FormatTokenEnum formatTokenEnum,
                          final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case DDD:
                    inputFragmentStr = matchStringOrDie(PATTERN_NUMBER, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.DAY_OF_YEAR, dateNr);
                    break;
                case DD:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.DAY_OF_MONTH, dateNr);
                    break;
                case D:
                    inputFragmentStr = matchStringOrDie(PATTERN_ONE_DIGIT, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.DAY_OF_MONTH, dateNr);
                    break;
                case DAY:
                    inputFragmentStr = setByName(result, params, Calendar.DAY_OF_WEEK, Calendar.LONG);
                    break;
                case DY:
                    inputFragmentStr = setByName(result, params, Calendar.DAY_OF_WEEK, Calendar.SHORT);
                    break;
                case J:
                    inputFragmentStr = matchStringOrDie(PATTERN_NUMBER, params, formatTokenEnum);
                    try {
                        Date date = new SimpleDateFormat("Myydd").parse(inputFragmentStr);
                        result.setTime(date);
                    } catch (ParseException e) {
                        throwException(params, format("Failed to parse Julian date: %s", inputFragmentStr));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    private static int MILLIS_in_hour = 60 * 60 * 1000;

    /**
     * Parslet responsible for parsing time parameter
     */
    private static final class TimeParslet implements ToDateParslet {

        @Override
        public void parse(final ToDateParser params, final FormatTokenEnum formatTokenEnum,
                          final String formatTokenStr) {
            final Calendar result = params.getResultCalendar();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
                case HH24:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.HOUR_OF_DAY, dateNr);
                    break;
                case HH12:
                case HH:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.HOUR, dateNr);
                    break;
                case MI:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.MINUTE, dateNr);
                    break;
                case SS:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.SECOND, dateNr);
                    break;
                case SSSSS:
                    inputFragmentStr = matchStringOrDie(PATTERN_NUMBER, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    result.set(Calendar.HOUR_OF_DAY, 0);
                    result.set(Calendar.MINUTE, 0);
                    result.set(Calendar.SECOND, dateNr);
                    break;
                case FF: //
                    inputFragmentStr = matchStringOrDie(PATTERN_NUMBER, params, formatTokenEnum);
                    String paddedRightNrStr = format("%-9s", inputFragmentStr).replace(' ', '0');
                    paddedRightNrStr = paddedRightNrStr.substring(0, 9);
                    Double nineDigits = Double.parseDouble(paddedRightNrStr);
                    params.setNanos(nineDigits.intValue());
                    dateNr = (int) Math.round(nineDigits / 1000000.0);
                    result.set(Calendar.MILLISECOND, dateNr);
                    break;
                case AM_PM:
                    inputFragmentStr = matchStringOrDie(PATTERN_AM_PM, params, formatTokenEnum);
                    if(inputFragmentStr.toUpperCase().startsWith("A")) {
                        result.set(Calendar.AM_PM, Calendar.AM);
                    }
                    else {
                        result.set(Calendar.AM_PM, Calendar.PM);
                    }
                    break;

                case TZH:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    TimeZone tz = result.getTimeZone();
                    int offsetMillis = tz.getRawOffset();
                    offsetMillis = (offsetMillis / MILLIS_in_hour) * MILLIS_in_hour; // purge min and sec
                    tz.setRawOffset(offsetMillis + dateNr);
                    result.setTimeZone(tz);
                    break;
                case TZM:
                    inputFragmentStr = matchStringOrDie(PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                    dateNr = parseInt(inputFragmentStr);
                    tz = result.getTimeZone();
                    offsetMillis = tz.getRawOffset();
                    offsetMillis = offsetMillis % MILLIS_in_hour; // purge hour
                    tz.setRawOffset(dateNr * MILLIS_in_hour + offsetMillis);
                    result.setTimeZone(tz);
                    break;
                case TZR: // Example: US/Pacific
                    final String s = params.getInputStr();
                    tz = result.getTimeZone();
                    for (String tzName : TimeZone.getAvailableIDs()) {
                        int length = tzName.length();
                        if (s.length() >= length && tzName.equalsIgnoreCase(s.substring(0, length))) {
                            tz.setID(tzName);
                            result.setTimeZone(tz);
                            inputFragmentStr = tzName;
                            break;
                        }
                    }
                    break;
                case TZD: // Must correspond with TZR region. Example: PST (for US/Pacific standard time)
                    throwException(params, format("token '%s' not supported yet.", formatTokenEnum.name()));
                    break;
                default:
                    throw new IllegalArgumentException(format("%s: Internal Error. Unhandled case: %s", this.getClass()
                            .getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    private static int parseInt(final String s) {
        int result = 0;
        if (s.length() > 0 && s.charAt(0) == '+') {
            result = Integer.parseInt(s.substring(1));
        } else {
            result = Integer.parseInt(s);
        }
        return result;
    }

    private static String matchStringOrDie(final Pattern p, final ToDateParser params, final Enum<?> aEnum) {
        final String s = params.getInputStr();
        Matcher matcher = p.matcher(s);
        if (!matcher.find()) {
            throwException(params, format("Issue happend when parsing token '%s'", aEnum.name()));
        }
        return matcher.group(1);
    }

    private static String setByName(final Calendar c, final ToDateParser params, final int field, final int style) {
        String inputFragmentStr = null;
        String s = params.getInputStr();
        Map<String, Integer> timeStringMap = c.getDisplayNames(field, style, Locale.getDefault());
        for (String dayName : timeStringMap.keySet()) {
            int leng = dayName.length();
            if (dayName.equalsIgnoreCase(s.substring(0, leng))) {
                c.set(field, timeStringMap.get(dayName));
                inputFragmentStr = dayName;
                break;
            }
        }
        if (inputFragmentStr == null || inputFragmentStr.isEmpty()) {
            throwException(params, format("Tried to parse one of '%s' but failed (may be an internal error?)",
                    timeStringMap.keySet()));
        }
        return inputFragmentStr;
    }

    private static void throwException(final ToDateParser params, final String errorStr) {
        throw DbException.get(
                ErrorCode.INVALID_TO_DATE_FORMAT,
                params.getFunctionName(),
                format(" %s. Details: %s", errorStr, params));
    }

}