/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Daniel Gredler
 */
package org.h2.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Currency;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Emulates Oracle's TO_CHAR function.
 */
public class ToChar {

    /**
     * The beginning of the Julian calendar.
     */
    private static final long JULIAN_EPOCH;

    static {
        GregorianCalendar epoch = new GregorianCalendar(Locale.ENGLISH);
        epoch.setGregorianChange(new Date(Long.MAX_VALUE));
        epoch.clear();
        epoch.set(4713, Calendar.JANUARY, 1, 0, 0, 0);
        epoch.set(Calendar.ERA, GregorianCalendar.BC);
        JULIAN_EPOCH = epoch.getTimeInMillis();
    }

    private ToChar() {
        // utility class
    }

    /**
     * Emulates Oracle's TO_CHAR(number) function.
     *
     * <p><table border="1">
     * <th><td>Input</td>
     * <td>Output</td>
     * <td>Closest {@link DecimalFormat} Equivalent</td></th>
     * <tr><td>,</td>
     * <td>Grouping separator.</td>
     * <td>,</td></tr>
     * <tr><td>.</td>
     * <td>Decimal separator.</td>
     * <td>.</td></tr>
     * <tr><td>$</td>
     * <td>Leading dollar sign.</td>
     * <td>$</td></tr>
     * <tr><td>0</td>
     * <td>Leading or trailing zeroes.</td>
     * <td>0</td></tr>
     * <tr><td>9</td>
     * <td>Digit.</td>
     * <td>#</td></tr>
     * <tr><td>B</td>
     * <td>Blanks integer part of a fixed point number less than 1.</td>
     * <td>#</td></tr>
     * <tr><td>C</td>
     * <td>ISO currency symbol.</td>
     * <td>\u00A4</td></tr>
     * <tr><td>D</td>
     * <td>Local decimal separator.</td>
     * <td>.</td></tr>
     * <tr><td>EEEE</td>
     * <td>Returns a value in scientific notation.</td>
     * <td>E</td></tr>
     * <tr><td>FM</td>
     * <td>Returns values with no leading or trailing spaces.</td>
     * <td>None.</td></tr>
     * <tr><td>G</td>
     * <td>Local grouping separator.</td>
     * <td>,</td></tr>
     * <tr><td>L</td>
     * <td>Local currency symbol.</td>
     * <td>\u00A4</td></tr>
     * <tr><td>MI</td>
     * <td>Negative values get trailing minus sign,
     * positive get trailing space.</td>
     * <td>-</td></tr>
     * <tr><td>PR</td>
     * <td>Negative values get enclosing angle brackets,
     * positive get spaces.</td>
     * <td>None.</td></tr>
     * <tr><td>RN</td>
     * <td>Returns values in Roman numerals.</td>
     * <td>None.</td></tr>
     * <tr><td>S</td>
     * <td>Returns values with leading/trailing +/- signs.</td>
     * <td>None.</td></tr>
     * <tr><td>TM</td>
     * <td>Returns smallest number of characters possible.</td>
     * <td>None.</td></tr>
     * <tr><td>U</td>
     * <td>Returns the dual currency symbol.</td>
     * <td>None.</td></tr>
     * <tr><td>V</td>
     * <td>Returns a value multiplied by 10^n.</td>
     * <td>None.</td></tr>
     * <tr><td>X</td>
     * <td>Hex value.</td>
     * <td>None.</td></tr>
     * </table>
     * See also TO_CHAR(number) and number format models
     * in the Oracle documentation.
     *
     * @param number the number to format
     * @param format the format pattern to use (if any)
     * @param nlsParam the NLS parameter (if any)
     * @return the formatted number
     */
    public static String toChar(BigDecimal number, String format,
            @SuppressWarnings("unused") String nlsParam) {

        // short-circuit logic for formats that don't follow common logic below
        String formatUp = format != null ? StringUtils.toUpperEnglish(format) : null;
        if (formatUp == null || formatUp.equals("TM") || formatUp.equals("TM9")) {
            String s = number.toPlainString();
            return s.startsWith("0.") ? s.substring(1) : s;
        } else if (formatUp.equals("TME")) {
            int pow = number.precision() - number.scale() - 1;
            number = number.movePointLeft(pow);
            return number.toPlainString() + "E" +
                    (pow < 0 ? '-' : '+') + (Math.abs(pow) < 10 ? "0" : "") + Math.abs(pow);
        } else if (formatUp.equals("RN")) {
            boolean lowercase = format.startsWith("r");
            String rn = StringUtils.pad(toRomanNumeral(number.intValue()), 15, " ", false);
            return lowercase ? rn.toLowerCase() : rn;
        } else if (formatUp.equals("FMRN")) {
            boolean lowercase = format.charAt(2) == 'r';
            String rn = toRomanNumeral(number.intValue());
            return lowercase ? rn.toLowerCase() : rn;
        } else if (formatUp.endsWith("X")) {
            return toHex(number, format);
        }

        String originalFormat = format;
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
        char localGrouping = symbols.getGroupingSeparator();
        char localDecimal = symbols.getDecimalSeparator();

        boolean leadingSign = formatUp.startsWith("S");
        if (leadingSign) {
            format = format.substring(1);
        }

        boolean trailingSign = formatUp.endsWith("S");
        if (trailingSign) {
            format = format.substring(0, format.length() - 1);
        }

        boolean trailingMinus = formatUp.endsWith("MI");
        if (trailingMinus) {
            format = format.substring(0, format.length() - 2);
        }

        boolean angleBrackets = formatUp.endsWith("PR");
        if (angleBrackets) {
            format = format.substring(0, format.length() - 2);
        }

        int v = formatUp.indexOf("V");
        if (v >= 0) {
            int digits = 0;
            for (int i = v + 1; i < format.length(); i++) {
                char c = format.charAt(i);
                if (c == '0' || c == '9') {
                    digits++;
                }
            }
            number = number.movePointRight(digits);
            format = format.substring(0, v) + format.substring(v + 1);
        }

        Integer power;
        if (format.endsWith("EEEE")) {
            power = number.precision() - number.scale() - 1;
            number = number.movePointLeft(power);
            format = format.substring(0, format.length() - 4);
        } else {
            power = null;
        }

        int maxLength = 1;
        boolean fillMode = !formatUp.startsWith("FM");
        if (!fillMode) {
            format = format.substring(2);
        }

        // blanks flag doesn't seem to actually do anything
        format = format.replaceAll("[Bb]", "");

        // if we need to round the number to fit into the format specified,
        // go ahead and do that first
        int separator = findDecimalSeparator(format);
        int formatScale = calculateScale(format, separator);
        if (formatScale < number.scale()) {
            number = number.setScale(formatScale, BigDecimal.ROUND_HALF_UP);
        }

        // any 9s to the left of the decimal separator but to the right of a
        // 0 behave the same as a 0, e.g. "09999.99" -> "00000.99"
        for (int i = format.indexOf('0'); i >= 0 && i < separator; i++) {
            if (format.charAt(i) == '9') {
                format = format.substring(0, i) + "0" + format.substring(i + 1);
            }
        }

        StringBuilder output = new StringBuilder();
        String unscaled = (number.abs().compareTo(BigDecimal.ONE) < 0 ?
                zeroesAfterDecimalSeparator(number) : "") +
                number.unscaledValue().abs().toString();

        // start at the decimal point and fill in the numbers to the left,
        // working our way from right to left
        int i = separator - 1;
        int j = unscaled.length() - number.scale() - 1;
        for (; i >= 0; i--) {
            char c = format.charAt(i);
            maxLength++;
            if (c == '9' || c == '0') {
                if (j >= 0) {
                    char digit = unscaled.charAt(j);
                    output.insert(0, digit);
                    j--;
                } else if (c == '0' && power == null) {
                    output.insert(0, '0');
                }
            } else if (c == ',') {
                // only add the grouping separator if we have more numbers
                if (j >= 0 || (i > 0 && format.charAt(i - 1) == '0')) {
                    output.insert(0, c);
                }
            } else if (c == 'G' || c == 'g') {
                // only add the grouping separator if we have more numbers
                if (j >= 0 || (i > 0 && format.charAt(i - 1) == '0')) {
                    output.insert(0, localGrouping);
                }
            } else if (c == 'C' || c == 'c') {
                Currency currency = Currency.getInstance(Locale.getDefault());
                output.insert(0, currency.getCurrencyCode());
                maxLength += 6;
            } else if (c == 'L' || c == 'l' || c == 'U' || c == 'u') {
                Currency currency = Currency.getInstance(Locale.getDefault());
                output.insert(0, currency.getSymbol());
                maxLength += 9;
            } else if (c == '$') {
                Currency currency = Currency.getInstance(Locale.getDefault());
                String cs = currency.getSymbol();
                output.insert(0, cs);
            } else {
                throw DbException.get(
                        ErrorCode.INVALID_TO_CHAR_FORMAT, originalFormat);
            }
        }

        // if the format (to the left of the decimal point) was too small
        // to hold the number, return a big "######" string
        if (j >= 0) {
            return StringUtils.pad("", format.length() + 1, "#", true);
        }

        if (separator < format.length()) {

            // add the decimal point
            maxLength++;
            char pt = format.charAt(separator);
            if (pt == 'd' || pt == 'D') {
                output.append(localDecimal);
            } else {
                output.append(pt);
            }

            // start at the decimal point and fill in the numbers to the right,
            // working our way from left to right
            i = separator + 1;
            j = unscaled.length() - number.scale();
            for (; i < format.length(); i++) {
                char c = format.charAt(i);
                maxLength++;
                if (c == '9' || c == '0') {
                    if (j < unscaled.length()) {
                        char digit = unscaled.charAt(j);
                        output.append(digit);
                        j++;
                    } else {
                        if (c == '0' || fillMode) {
                            output.append('0');
                        }
                    }
                } else {
                    throw DbException.get(
                            ErrorCode.INVALID_TO_CHAR_FORMAT, originalFormat);
                }
            }
        }

        addSign(output, number.signum(), leadingSign, trailingSign,
                trailingMinus, angleBrackets, fillMode);

        if (power != null) {
            output.append('E');
            output.append(power < 0 ? '-' : '+');
            output.append(Math.abs(power) < 10 ? "0" : "");
            output.append(Math.abs(power));
        }

        if (fillMode) {
            if (power != null) {
                output.insert(0, ' ');
            } else {
                while (output.length() < maxLength) {
                    output.insert(0, ' ');
                }
            }
        }

        return output.toString();
    }

    private static String zeroesAfterDecimalSeparator(BigDecimal number) {
        final String numberStr = number.toString();
        final int idx = numberStr.indexOf('.');
        if (idx < 0) {
            return "";
        }
        int i = idx + 1;
        boolean allZeroes = true;
        for (; i < numberStr.length(); i++) {
            if (numberStr.charAt(i) != '0') {
                allZeroes = false;
                break;
            }
        }
        final char[] zeroes = new char[allZeroes ? numberStr.length() - idx - 1: i - 1 - idx];
        Arrays.fill(zeroes, '0');
        return String.valueOf(zeroes);
    }

    private static void addSign(StringBuilder output, int signum,
            boolean leadingSign, boolean trailingSign, boolean trailingMinus,
            boolean angleBrackets, boolean fillMode) {
        if (angleBrackets) {
            if (signum < 0) {
                output.insert(0, '<');
                output.append('>');
            } else if (fillMode) {
                output.insert(0, ' ');
                output.append(' ');
            }
        } else {
            String sign;
            if (signum == 0) {
                sign = "";
            } else if (signum < 0) {
                sign = "-";
            } else {
                if (leadingSign || trailingSign) {
                    sign = "+";
                } else if (fillMode) {
                    sign = " ";
                } else {
                    sign = "";
                }
            }
            if (trailingMinus || trailingSign) {
                output.append(sign);
            } else {
                output.insert(0, sign);
            }
        }
    }

    private static int findDecimalSeparator(String format) {
        int index = format.indexOf('.');
        if (index == -1) {
            index = format.indexOf('D');
            if (index == -1) {
                index = format.indexOf('d');
                if (index == -1) {
                    index = format.length();
                }
            }
        }
        return index;
    }

    private static int calculateScale(String format, int separator) {
        int scale = 0;
        for (int i = separator; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == '0' || c == '9') {
                scale++;
            }
        }
        return scale;
    }

    private static String toRomanNumeral(int number) {
        int[] values = new int[] { 1000, 900, 500, 400, 100, 90, 50, 40, 10, 9,
                5, 4, 1 };
        String[] numerals = new String[] { "M", "CM", "D", "CD", "C", "XC",
                "L", "XL", "X", "IX", "V", "IV", "I" };
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            int value = values[i];
            String numeral = numerals[i];
            while (number >= value) {
                result.append(numeral);
                number -= value;
            }
        }
        return result.toString();
    }

    private static String toHex(BigDecimal number, String format) {

        boolean fillMode = !StringUtils.toUpperEnglish(format).startsWith("FM");
        boolean uppercase = !format.contains("x");
        boolean zeroPadded = format.startsWith("0");
        int digits = 0;
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == '0' || c == 'X' || c == 'x') {
                digits++;
            }
        }

        int i = number.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
        String hex = Integer.toHexString(i);
        if (digits < hex.length()) {
            hex = StringUtils.pad("", digits + 1, "#", true);
        } else {
            if (uppercase) {
                hex = StringUtils.toUpperEnglish(hex);
            }
            if (zeroPadded) {
                hex = StringUtils.pad(hex, digits, "0", false);
            }
            if (fillMode) {
                hex = StringUtils.pad(hex, format.length() + 1, " ", false);
            }
        }

        return hex;
    }

    /**
     * Emulates Oracle's TO_CHAR(datetime) function.
     *
     * <p><table border="1">
     * <th><td>Input</td>
     * <td>Output</td>
     * <td>Closest {@link SimpleDateFormat} Equivalent</td></th>
     * <tr><td>- / , . ; : "text"</td>
     * <td>Reproduced verbatim.</td>
     * <td>'text'</td></tr>
     * <tr><td>A.D. AD B.C. BC</td>
     * <td>Era designator, with or without periods.</td>
     * <td>G</td></tr>
     * <tr><td>A.M. AM P.M. PM</td>
     * <td>AM/PM marker.</td>
     * <td>a</td></tr>
     * <tr><td>CC SCC</td>
     * <td>Century.</td>
     * <td>None.</td></tr>
     * <tr><td>D</td>
     * <td>Day of week.</td>
     * <td>u</td></tr>
     * <tr><td>DAY</td>
     * <td>Name of day.</td>
     * <td>EEEE</td></tr>
     * <tr><td>DY</td>
     * <td>Abbreviated day name.</td>
     * <td>EEE</td></tr>
     * <tr><td>DD</td>
     * <td>Day of month.</td>
     * <td>d</td></tr>
     * <tr><td>DDD</td>
     * <td>Day of year.</td>
     * <td>D</td></tr>
     * <tr><td>DL</td>
     * <td>Long date format.</td>
     * <td>EEEE, MMMM d, yyyy</td></tr>
     * <tr><td>DS</td>
     * <td>Short date format.</td>
     * <td>MM/dd/yyyy</td></tr>
     * <tr><td>E</td>
     * <td>Abbreviated era name (Japanese, Chinese, Thai)</td>
     * <td>None.</td></tr>
     * <tr><td>EE</td>
     * <td>Full era name (Japanese, Chinese, Thai)</td>
     * <td>None.</td></tr>
     * <tr><td>FF[1-9]</td>
     * <td>Fractional seconds.</td>
     * <td>S</td></tr>
     * <tr><td>FM</td>
     * <td>Returns values with no leading or trailing spaces.</td>
     * <td>None.</td></tr>
     * <tr><td>FX</td>
     * <td>Requires exact matches between character data and format model.</td>
     * <td>None.</td></tr>
     * <tr><td>HH HH12</td>
     * <td>Hour in AM/PM (1-12).</td>
     * <td>hh</td></tr>
     * <tr><td>HH24</td>
     * <td>Hour in day (0-23).</td>
     * <td>HH</td></tr>
     * <tr><td>IW</td>
     * <td>Week in year.</td>
     * <td>w</td></tr>
     * <tr><td>WW</td>
     * <td>Week in year.</td>
     * <td>w</td></tr>
     * <tr><td>W</td>
     * <td>Week in month.</td>
     * <td>W</td></tr>
     * <tr><td>IYYY IYY IY I</td>
     * <td>Last 4/3/2/1 digit(s) of ISO year.</td>
     * <td>yyyy yyy yy y</td></tr>
     * <tr><td>RRRR RR</td>
     * <td>Last 4/2 digits of year.</td>
     * <td>yyyy yy</td></tr>
     * <tr><td>Y,YYY</td>
     * <td>Year with comma.</td>
     * <td>None.</td></tr>
     * <tr><td>YEAR SYEAR</td>
     * <td>Year spelled out (S prefixes BC years with minus sign).</td>
     * <td>None.</td></tr>
     * <tr><td>YYYY SYYYY</td>
     * <td>4-digit year (S prefixes BC years with minus sign).</td>
     * <td>yyyy</td></tr>
     * <tr><td>YYY YY Y</td>
     * <td>Last 3/2/1 digit(s) of year.</td>
     * <td>yyy yy y</td></tr>
     * <tr><td>J</td>
     * <td>Julian day (number of days since January 1, 4712 BC).</td>
     * <td>None.</td></tr>
     * <tr><td>MI</td>
     * <td>Minute in hour.</td>
     * <td>mm</td></tr>
     * <tr><td>MM</td>
     * <td>Month in year.</td>
     * <td>MM</td></tr>
     * <tr><td>MON</td>
     * <td>Abbreviated name of month.</td>
     * <td>MMM</td></tr>
     * <tr><td>MONTH</td>
     * <td>Name of month, padded with spaces.</td>
     * <td>MMMM</td></tr>
     * <tr><td>RM</td>
     * <td>Roman numeral month.</td>
     * <td>None.</td></tr>
     * <tr><td>Q</td>
     * <td>Quarter of year.</td>
     * <td>None.</td></tr>
     * <tr><td>SS</td>
     * <td>Seconds in minute.</td>
     * <td>ss</td></tr>
     * <tr><td>SSSSS</td>
     * <td>Seconds in day.</td>
     * <td>None.</td></tr>
     * <tr><td>TS</td>
     * <td>Short time format.</td>
     * <td>h:mm:ss aa</td></tr>
     * <tr><td>TZD</td>
     * <td>Daylight savings time zone abbreviation.</td>
     * <td>z</td></tr>
     * <tr><td>TZR</td>
     * <td>Time zone region information.</td>
     * <td>zzzz</td></tr>
     * <tr><td>X</td>
     * <td>Local radix character.</td>
     * <td>None.</td></tr>
     * </table>
     * <p>
     * See also TO_CHAR(datetime) and datetime format models
     * in the Oracle documentation.
     *
     * @param ts the timestamp to format
     * @param format the format pattern to use (if any)
     * @param nlsParam the NLS parameter (if any)
     * @return the formatted timestamp
     */
    public static String toChar(Timestamp ts, String format, @SuppressWarnings("unused") String nlsParam) {

        if (format == null) {
            format = "DD-MON-YY HH.MI.SS.FF PM";
        }

        GregorianCalendar cal = new GregorianCalendar(Locale.ENGLISH);
        cal.setTimeInMillis(ts.getTime());
        StringBuilder output = new StringBuilder();
        boolean fillMode = true;

        for (int i = 0; i < format.length();) {

            Capitalization cap;

                // AD / BC

            if ((cap = containsAt(format, i, "A.D.", "B.C.")) != null) {
                String era = cal.get(Calendar.ERA) == GregorianCalendar.AD ? "A.D." : "B.C.";
                output.append(cap.apply(era));
                i += 4;
            } else if ((cap = containsAt(format, i, "AD", "BC")) != null) {
                String era = cal.get(Calendar.ERA) == GregorianCalendar.AD ? "AD" : "BC";
                output.append(cap.apply(era));
                i += 2;

                // AM / PM

            } else if ((cap = containsAt(format, i, "A.M.", "P.M.")) != null) {
                String am = cal.get(Calendar.AM_PM) == Calendar.AM ? "A.M." : "P.M.";
                output.append(cap.apply(am));
                i += 4;
            } else if ((cap = containsAt(format, i, "AM", "PM")) != null) {
                String am = cal.get(Calendar.AM_PM) == Calendar.AM ? "AM" : "PM";
                output.append(cap.apply(am));
                i += 2;

                // Long/short date/time format

            } else if ((cap = containsAt(format, i, "DL")) != null) {
                output.append(new SimpleDateFormat("EEEE, MMMM d, yyyy").format(ts));
                i += 2;
            } else if ((cap = containsAt(format, i, "DS")) != null) {
                output.append(new SimpleDateFormat("MM/dd/yyyy").format(ts));
                i += 2;
            } else if ((cap = containsAt(format, i, "TS")) != null) {
                output.append(new SimpleDateFormat("h:mm:ss aa").format(ts));
                i += 2;

                // Day

            } else if ((cap = containsAt(format, i, "DDD")) != null) {
                output.append(cal.get(Calendar.DAY_OF_YEAR));
                i += 3;
            } else if ((cap = containsAt(format, i, "DD")) != null) {
                output.append(String.format("%02d",
                        cal.get(Calendar.DAY_OF_MONTH)));
                i += 2;
            } else if ((cap = containsAt(format, i, "DY")) != null) {
                String day = StringUtils.toUpperEnglish(new SimpleDateFormat("EEE").format(ts));
                output.append(cap.apply(day));
                i += 2;
            } else if ((cap = containsAt(format, i, "DAY")) != null) {
                String day = new SimpleDateFormat("EEEE").format(ts);
                if (fillMode) {
                    day = StringUtils.pad(day, "Wednesday".length(), " ", true);
                }
                output.append(cap.apply(day));
                i += 3;
            } else if ((cap = containsAt(format, i, "D")) != null) {
                output.append(cal.get(Calendar.DAY_OF_WEEK));
                i += 1;
            } else if ((cap = containsAt(format, i, "J")) != null) {
                long millis = ts.getTime() - JULIAN_EPOCH;
                long days = (long) Math.floor(millis / (1000 * 60 * 60 * 24));
                output.append(days);
                i += 1;

                // Hours

            } else if ((cap = containsAt(format, i, "HH24")) != null) {
                output.append(new DecimalFormat("00").format(cal.get(Calendar.HOUR_OF_DAY)));
                i += 4;
            } else if ((cap = containsAt(format, i, "HH12")) != null) {
                output.append(new DecimalFormat("00").format(cal.get(Calendar.HOUR)));
                i += 4;
            } else if ((cap = containsAt(format, i, "HH")) != null) {
                output.append(new DecimalFormat("00").format(cal.get(Calendar.HOUR)));
                i += 2;

                // Minutes

            } else if ((cap = containsAt(format, i, "MI")) != null) {
                output.append(new DecimalFormat("00").format(cal.get(Calendar.MINUTE)));
                i += 2;

                // Seconds

            } else if ((cap = containsAt(format, i, "SSSSS")) != null) {
                int seconds = cal.get(Calendar.HOUR_OF_DAY) * 60 * 60;
                seconds += cal.get(Calendar.MINUTE) * 60;
                seconds += cal.get(Calendar.SECOND);
                output.append(seconds);
                i += 5;
            } else if ((cap = containsAt(format, i, "SS")) != null) {
                output.append(new DecimalFormat("00").format(cal.get(Calendar.SECOND)));
                i += 2;

                // Fractional seconds

            } else if ((cap = containsAt(format, i, "FF1", "FF2",
                    "FF3", "FF4", "FF5", "FF6", "FF7", "FF8", "FF9")) != null) {
                int x = Integer.parseInt(format.substring(i + 2, i + 3));
                int ff = (int) (cal.get(Calendar.MILLISECOND) * Math.pow(10, x - 3));
                output.append(ff);
                i += 3;
            } else if ((cap = containsAt(format, i, "FF")) != null) {
                output.append(cal.get(Calendar.MILLISECOND) * 1000);
                i += 2;

                // Time zone

            } else if ((cap = containsAt(format, i, "TZR")) != null) {
                TimeZone tz = TimeZone.getDefault();
                output.append(tz.getID());
                i += 3;
            } else if ((cap = containsAt(format, i, "TZD")) != null) {
                TimeZone tz = TimeZone.getDefault();
                boolean daylight = tz.inDaylightTime(new java.util.Date());
                output.append(tz.getDisplayName(daylight, TimeZone.SHORT));
                i += 3;

                // Week

            } else if ((cap = containsAt(format, i, "IW", "WW")) != null) {
                output.append(cal.get(Calendar.WEEK_OF_YEAR));
                i += 2;
            } else if ((cap = containsAt(format, i, "W")) != null) {
                int w = (int) (1 + Math.floor(cal.get(Calendar.DAY_OF_MONTH) / 7));
                output.append(w);
                i += 1;

                // Year

            } else if ((cap = containsAt(format, i, "Y,YYY")) != null) {
                output.append(new DecimalFormat("#,###").format(getYear(cal)));
                i += 5;
            } else if ((cap = containsAt(format, i, "SYYYY")) != null) {
                if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
                    output.append('-');
                }
                output.append(new DecimalFormat("0000").format(getYear(cal)));
                i += 5;
            } else if ((cap = containsAt(format, i, "YYYY", "IYYY", "RRRR")) != null) {
                output.append(new DecimalFormat("0000").format(getYear(cal)));
                i += 4;
            } else if ((cap = containsAt(format, i, "YYY", "IYY")) != null) {
                output.append(new DecimalFormat("000").format(getYear(cal) % 1000));
                i += 3;
            } else if ((cap = containsAt(format, i, "YY", "IY", "RR")) != null) {
                output.append(new DecimalFormat("00").format(getYear(cal) % 100));
                i += 2;
            } else if ((cap = containsAt(format, i, "I", "Y")) != null) {
                output.append(getYear(cal) % 10);
                i += 1;

                // Month / quarter

            } else if ((cap = containsAt(format, i, "MONTH")) != null) {
                String month = new SimpleDateFormat("MMMM").format(ts);
                if (fillMode) {
                    month = StringUtils.pad(month, "September".length(), " ", true);
                }
                output.append(cap.apply(month));
                i += 5;
            } else if ((cap = containsAt(format, i, "MON")) != null) {
                String month = new SimpleDateFormat("MMM").format(ts);
                output.append(cap.apply(month));
                i += 3;
            } else if ((cap = containsAt(format, i, "MM")) != null) {
                output.append(String.format("%02d", cal.get(Calendar.MONTH) + 1));
                i += 2;
            } else if ((cap = containsAt(format, i, "RM")) != null) {
                int month = cal.get(Calendar.MONTH) + 1;
                output.append(cap.apply(toRomanNumeral(month)));
                i += 2;
            } else if ((cap = containsAt(format, i, "Q")) != null) {
                int q = (int) (1 + Math.floor(cal.get(Calendar.MONTH) / 3));
                output.append(q);
                i += 1;

                // Local radix character

            } else if ((cap = containsAt(format, i, "X")) != null) {
                char c = DecimalFormatSymbols.getInstance().getDecimalSeparator();
                output.append(c);
                i += 1;

                // Format modifiers

            } else if ((cap = containsAt(format, i, "FM")) != null) {
                fillMode = !fillMode;
                i += 2;
            } else if ((cap = containsAt(format, i, "FX")) != null) {
                i += 2;

                // Literal text

            } else if ((cap = containsAt(format, i, "\"")) != null) {
                for (i = i + 1; i < format.length(); i++) {
                    char c = format.charAt(i);
                    if (c != '"') {
                        output.append(c);
                    } else {
                        i++;
                        break;
                    }
                }
            } else if (format.charAt(i) == '-'
                    || format.charAt(i) == '/'
                    || format.charAt(i) == ','
                    || format.charAt(i) == '.'
                    || format.charAt(i) == ';'
                    || format.charAt(i) == ':'
                    || format.charAt(i) == ' ') {
                output.append(format.charAt(i));
                i += 1;

                // Anything else

            } else {
                throw DbException.get(ErrorCode.INVALID_TO_CHAR_FORMAT, format);
            }
        }

        return output.toString();
    }

    private static int getYear(Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
            year--;
        }
        return year;
    }

    /**
     * Returns a capitalization strategy if the specified string contains any of
     * the specified substrings at the specified index. The capitalization
     * strategy indicates the casing of the substring that was found. If none of
     * the specified substrings are found, this method returns <code>null</code>
     * .
     *
     * @param s the string to check
     * @param index the index to check at
     * @param substrings the substrings to check for within the string
     * @return a capitalization strategy if the specified string contains any of
     *         the specified substrings at the specified index,
     *         <code>null</code> otherwise
     */
    private static Capitalization containsAt(String s, int index,
            String... substrings) {
        for (String substring : substrings) {
            if (index + substring.length() <= s.length()) {
                boolean found = true;
                Boolean up1 = null;
                Boolean up2 = null;
                for (int i = 0; i < substring.length(); i++) {
                    char c1 = s.charAt(index + i);
                    char c2 = substring.charAt(i);
                    if (c1 != c2 && Character.toUpperCase(c1) != Character.toUpperCase(c2)) {
                        found = false;
                        break;
                    } else if (Character.isLetter(c1)) {
                        if (up1 == null) {
                            up1 = Character.isUpperCase(c1);
                        } else if (up2 == null) {
                            up2 = Character.isUpperCase(c1);
                        }
                    }
                }
                if (found) {
                    return Capitalization.toCapitalization(up1, up2);
                }
            }
        }
        return null;
    }

    /** Represents a capitalization / casing strategy. */
    private enum Capitalization {

        /**
         * All letters are uppercased.
         */
        UPPERCASE,

        /**
         * All letters are lowercased.
         */
        LOWERCASE,

        /**
         * The string is capitalized (first letter uppercased, subsequent
         * letters lowercased).
         */
        CAPITALIZE;

        /**
         * Returns the capitalization / casing strategy which should be used
         * when the first and second letters have the specified casing.
         *
         * @param up1 whether or not the first letter is uppercased
         * @param up2 whether or not the second letter is uppercased
         * @return the capitalization / casing strategy which should be used
         *         when the first and second letters have the specified casing
         */
        public static Capitalization toCapitalization(Boolean up1, Boolean up2) {
            if (up1 == null) {
                return Capitalization.CAPITALIZE;
            } else if (up2 == null) {
                return up1 ? Capitalization.UPPERCASE : Capitalization.LOWERCASE;
            } else if (up1) {
                return up2 ? Capitalization.UPPERCASE : Capitalization.CAPITALIZE;
            } else {
                return Capitalization.LOWERCASE;
            }
        }

        /**
         * Applies this capitalization strategy to the specified string.
         *
         * @param s the string to apply this strategy to
         * @return the resultant string
         */
        public String apply(String s) {
            if (s == null || s.isEmpty()) {
                return s;
            }
            switch (this) {
            case UPPERCASE:
                return StringUtils.toUpperEnglish(s);
            case LOWERCASE:
                return StringUtils.toLowerEnglish(s);
            case CAPITALIZE:
                return Character.toUpperCase(s.charAt(0)) +
                        (s.length() > 1 ? StringUtils.toLowerEnglish(s).substring(1) : "");
            default:
                throw new IllegalArgumentException(
                        "Unknown capitalization strategy: " + this);
            }
        }
    }
}
