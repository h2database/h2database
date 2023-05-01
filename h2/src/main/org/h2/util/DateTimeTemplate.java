/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.util.DateTimeTemplate.FieldType.AMPM;
import static org.h2.util.DateTimeTemplate.FieldType.DAY_OF_MONTH;
import static org.h2.util.DateTimeTemplate.FieldType.DAY_OF_YEAR;
import static org.h2.util.DateTimeTemplate.FieldType.DELIMITER;
import static org.h2.util.DateTimeTemplate.FieldType.FRACTION;
import static org.h2.util.DateTimeTemplate.FieldType.HOUR12;
import static org.h2.util.DateTimeTemplate.FieldType.HOUR24;
import static org.h2.util.DateTimeTemplate.FieldType.MINUTE;
import static org.h2.util.DateTimeTemplate.FieldType.MONTH;
import static org.h2.util.DateTimeTemplate.FieldType.ROUNDED_YEAR;
import static org.h2.util.DateTimeTemplate.FieldType.SECOND_OF_DAY;
import static org.h2.util.DateTimeTemplate.FieldType.SECOND_OF_MINUTE;
import static org.h2.util.DateTimeTemplate.FieldType.TIME_ZONE_HOUR;
import static org.h2.util.DateTimeTemplate.FieldType.TIME_ZONE_MINUTE;
import static org.h2.util.DateTimeTemplate.FieldType.TIME_ZONE_SECOND;
import static org.h2.util.DateTimeTemplate.FieldType.YEAR;
import static org.h2.util.DateTimeUtils.FRACTIONAL_SECONDS_TABLE;
import static org.h2.util.DateTimeUtils.*;
import static org.h2.util.DateTimeUtils.NANOS_PER_MINUTE;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import java.util.ArrayList;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Date-time template.
 */
public final class DateTimeTemplate {

    public static final class FieldType {

        static final int YEAR = 0, ROUNDED_YEAR = 1, MONTH = 2, DAY_OF_MONTH = 3, DAY_OF_YEAR = 4;

        static final int HOUR12 = 5, HOUR24 = 6, MINUTE = 7, SECOND_OF_MINUTE = 8, SECOND_OF_DAY = 9, FRACTION = 10,
                AMPM = 11;

        static final int TIME_ZONE_HOUR = 12, TIME_ZONE_MINUTE = 13, TIME_ZONE_SECOND = 14;

        static final int DELIMITER = 15;

    }

    private static final class Scanner {

        final String string;

        private int offset;

        private final int length;

        Scanner(String string) {
            this.string = string;
            this.length = string.length();
        }

        int readChar() {
            return offset < length ? string.charAt(offset++) : -1;
        }

        void readChar(char c) {
            if (offset >= length || string.charAt(offset) != c) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
            }
            offset++;
        }

        boolean readCharIf(char c) {
            if (offset < length && string.charAt(offset) == c) {
                offset++;
                return true;
            }
            return false;
        }

        int readPositiveInt(int digits, boolean delimited) {
            int start = offset, end;
            if (delimited) {
                end = start;
                for (char c; end < length && (c = string.charAt(end)) >= '0' && c <= '9'; end++) {
                }
                if (start == end) {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
                }
            } else {
                end = start + digits;
                if (end > length) {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
                }
            }
            try {
                return StringUtils.parseUInt31(string, start, offset = end);
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
            }
        }

        int readNanos(int digits, boolean delimited) {
            int start = offset, end = start;
            int nanos = 0, mul = 100_000_000;
            if (delimited) {
                end = start;
                for (char c; end < length && (c = string.charAt(end)) >= '0' && c <= '9'; end++) {
                    nanos += mul * (c - '0');
                    mul /= 10;
                }
                if (start == end) {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
                }
            } else {
                end = start + digits;
                if (end > length) {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
                }
                for (; start < end; start++) {
                    char c = string.charAt(start);
                    if (c < '0' || c > '9') {
                        throw DbException.get(ErrorCode.PARSE_ERROR_1, string);
                    }
                    nanos += mul * (c - '0');
                    mul /= 10;
                }
            }
            offset = end;
            return nanos;
        }

    }

    private static abstract class Part {

        Part() {
        }

        abstract int type();

        abstract void format(StringBuilder builder, long dateValue, long timeNanos, int offsetSeconds);

        abstract void parse(int[] target, Scanner s, boolean delimited, int year);

    }

    private static final class Delimiter extends Part {

        static final Delimiter MINUS_SIGN = new Delimiter('-'), PERIOD = new Delimiter('.'),
                SOLIDUS = new Delimiter('/'), COMMA = new Delimiter(','), APOSTROPHE = new Delimiter('\''),
                SEMICOLON = new Delimiter(';'), COLON = new Delimiter(':'), SPACE = new Delimiter(' ');

        private final char delimiter;

        private Delimiter(char delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        int type() {
            return DELIMITER;
        }

        @Override
        public void format(StringBuilder builder, long dateValue, long timeNanos, int offsetSeconds) {
            builder.append(delimiter);
        }

        @Override
        public void parse(int[] target, Scanner s, boolean delimited, int year) {
            s.readChar(delimiter);
        }

    }

    private static final class Field extends Part {

        static final Field Y = new Field(YEAR, 1), YY = new Field(YEAR, 2), YYY = new Field(YEAR, 3),
                YYYY = new Field(YEAR, 4);

        static final Field RR = new Field(ROUNDED_YEAR, 2), RRRR = new Field(ROUNDED_YEAR, 4);

        static final Field MM = new Field(MONTH, 2);

        static final Field DD = new Field(DAY_OF_MONTH, 2);

        static final Field DDD = new Field(DAY_OF_YEAR, 3);

        static final Field HH12 = new Field(HOUR12, 2);

        static final Field HH24 = new Field(HOUR24, 2);

        static final Field MI = new Field(MINUTE, 2);

        static final Field SS = new Field(SECOND_OF_MINUTE, 2);

        static final Field SSSSS = new Field(SECOND_OF_DAY, 5);

        private static final Field FF[];

        static final Field AM_PM = new Field(AMPM, 4);

        static final Field TZH = new Field(TIME_ZONE_HOUR, 2);

        static final Field TZM = new Field(TIME_ZONE_MINUTE, 2);

        static final Field TZS = new Field(TIME_ZONE_SECOND, 2);

        static {
            Field[] ff = new Field[9];
            for (int i = 0; i < 9;) {
                ff[i] = new Field(FRACTION, ++i);
            }
            FF = ff;
        }

        static Field ff(int digits) {
            return FF[digits - 1];
        }

        private final int type;

        private final int digits;

        Field(int type, int digits) {
            this.type = type;
            this.digits = digits;
        }

        @Override
        int type() {
            return type;
        }

        @Override
        void format(StringBuilder builder, long dateValue, long timeNanos, int offsetSeconds) {
            switch (type) {
            case YEAR:
            case ROUNDED_YEAR: {
                int y = DateTimeUtils.yearFromDateValue(dateValue);
                if (y < 0) {
                    builder.append('-');
                    y = -y;
                }
                switch (digits) {
                case 1:
                    y %= 10;
                    break;
                case 2:
                    y %= 100;
                    break;
                case 3:
                    y %= 1_000;
                }
                formatLast(builder, y, digits);
                break;
            }
            case MONTH:
                StringUtils.appendTwoDigits(builder, DateTimeUtils.monthFromDateValue(dateValue));
                break;
            case DAY_OF_MONTH:
                StringUtils.appendTwoDigits(builder, DateTimeUtils.dayFromDateValue(dateValue));
                break;
            case DAY_OF_YEAR:
                StringUtils.appendZeroPadded(builder, 3, DateTimeUtils.getDayOfYear(dateValue));
                break;
            case HOUR12: {
                int h = (int) (timeNanos / NANOS_PER_HOUR);
                if (h == 0) {
                    h = 12;
                } else if (h > 12) {
                    h -= 12;
                }
                StringUtils.appendTwoDigits(builder, h);
                break;
            }
            case HOUR24:
                StringUtils.appendTwoDigits(builder, (int) (timeNanos / NANOS_PER_HOUR));
                break;
            case MINUTE:
                StringUtils.appendTwoDigits(builder, (int) (timeNanos / NANOS_PER_MINUTE % 60));
                break;
            case SECOND_OF_MINUTE:
                StringUtils.appendTwoDigits(builder, (int) (timeNanos / NANOS_PER_SECOND % 60));
                break;
            case SECOND_OF_DAY:
                StringUtils.appendZeroPadded(builder, 5, (int) (timeNanos / NANOS_PER_SECOND));
                break;
            case FRACTION:
                formatLast(builder, (int) (timeNanos % NANOS_PER_SECOND) / FRACTIONAL_SECONDS_TABLE[digits], digits);
                break;
            case AMPM: {
                int h = (int) (timeNanos / NANOS_PER_HOUR);
                builder.append(h < 12 ? "A.M." : "P.M.");
                break;
            }
            case TIME_ZONE_HOUR: {
                int h = offsetSeconds / 3_600;
                if (offsetSeconds >= 0) {
                    builder.append('+');
                } else {
                    h = -h;
                    builder.append('-');
                }
                StringUtils.appendTwoDigits(builder, h);
                break;
            }
            case TIME_ZONE_MINUTE:
                StringUtils.appendTwoDigits(builder, Math.abs(offsetSeconds % 3_600 / 60));
                break;
            case TIME_ZONE_SECOND: {
                StringUtils.appendTwoDigits(builder, Math.abs(offsetSeconds % 60));
            }
            }
        }

        private static void formatLast(StringBuilder builder, int value, int digits) {
            if (digits == 2) {
                StringUtils.appendTwoDigits(builder, value);
            } else {
                StringUtils.appendZeroPadded(builder, digits, value);
            }
        }

        @Override
        void parse(int[] target, Scanner s, boolean delimited, int year) {
            switch (type) {
            case YEAR:
            case ROUNDED_YEAR: {
                boolean negative = s.readCharIf('-');
                if (!negative) {
                    s.readCharIf('+');
                }
                int v = s.readPositiveInt(digits, delimited);
                if (negative) {
                    if (digits < 4 || type == ROUNDED_YEAR) {
                        throw DbException.get(ErrorCode.PARSE_ERROR_1, s.string);
                    }
                    v = -v;
                } else if (digits < 4) {
                    if (digits == 1) {
                        if (v > 9) {
                            throw DbException.get(ErrorCode.PARSE_ERROR_1, s.string);
                        }
                        v += year / 10 * 10;
                    } else if (digits == 2) {
                        if (v > 99) {
                            throw DbException.get(ErrorCode.PARSE_ERROR_1, s.string);
                        }
                        v += year / 100 * 100;
                        if (type == ROUNDED_YEAR) {
                            if (v > year + 50) {
                                v -= 100;
                            } else if (v < year - 49) {
                                year += 100;
                            }
                        }
                    } else if (digits == 3) {
                        if (v > 999) {
                            throw DbException.get(ErrorCode.PARSE_ERROR_1, s.string);
                        }
                        v += year / 1_000 * 1_000;
                    }
                }
                target[type] = v;
                break;
            }
            case MONTH:
            case DAY_OF_MONTH:
            case DAY_OF_YEAR:
            case HOUR12:
            case HOUR24:
            case MINUTE:
            case SECOND_OF_MINUTE:
            case SECOND_OF_DAY:
            case TIME_ZONE_MINUTE:
            case TIME_ZONE_SECOND:
                target[type] = s.readPositiveInt(digits, delimited);
                break;
            case FRACTION:
                target[FRACTION] = s.readNanos(digits, delimited);
                break;
            case AMPM: {
                int v;
                if (s.readCharIf('A')) {
                    v = 0;
                } else {
                    s.readChar('P');
                    v = 1;
                }
                s.readChar('.');
                s.readChar('M');
                s.readChar('.');
                target[AMPM] = v;
                break;
            }
            case TIME_ZONE_HOUR: {
                boolean negative = s.readCharIf('-');
                if (!negative) {
                    if (!s.readCharIf('+')) {
                        s.readChar(' ');
                    }
                }
                int v = s.readPositiveInt(digits, delimited);
                if (v > 18) {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, s.string);
                }
                target[TIME_ZONE_HOUR] = negative ? (v == 0 ? -100 : -v) : v;
            }
            }
        }

    }

    private static final SmallLRUCache<String, DateTimeTemplate> CACHE = SmallLRUCache.newInstance(100);

    public static DateTimeTemplate of(String template) {
        synchronized (CACHE) {
            DateTimeTemplate t = CACHE.get(template);
            if (t != null) {
                return t;
            }
        }
        DateTimeTemplate t = parseTemplate(template), old;
        synchronized (CACHE) {
            old = CACHE.putIfAbsent(template, t);
        }
        return old != null ? old : t;
    }

    private static DateTimeTemplate parseTemplate(String template) {
        ArrayList<Part> parts = new ArrayList<>();
        Scanner s = new Scanner(template);
        int usedFields = 0;
        for (int c; (c = s.readChar()) >= 0;) {
            Part part;
            switch (c) {
            case '-':
                part = Delimiter.MINUS_SIGN;
                break;
            case '.':
                part = Delimiter.PERIOD;
                break;
            case '/':
                part = Delimiter.SOLIDUS;
                break;
            case ',':
                part = Delimiter.COMMA;
                break;
            case '\'':
                part = Delimiter.APOSTROPHE;
                break;
            case ';':
                part = Delimiter.SEMICOLON;
                break;
            case ':':
                part = Delimiter.COLON;
                break;
            case ' ':
                part = Delimiter.SPACE;
                break;
            case 'Y':
                usedFields = checkUsed(usedFields, YEAR, template);
                if (s.readCharIf('Y')) {
                    if (s.readCharIf('Y')) {
                        part = s.readCharIf('Y') ? Field.YYYY : Field.YYY;
                    } else {
                        part = Field.YY;
                    }
                } else {
                    part = Field.Y;
                }
                break;
            case 'R':
                // Year and rounded year may not be used together, mark both as
                // YEAR
                usedFields = checkUsed(usedFields, YEAR, template);
                s.readChar('R');
                if (s.readCharIf('R')) {
                    s.readChar('R');
                    part = Field.RRRR;
                } else {
                    part = Field.RR;
                }
                break;
            case 'M':
                if (s.readCharIf('I')) {
                    usedFields = checkUsed(usedFields, MINUTE, template);
                    part = Field.MI;
                } else {
                    s.readChar('M');
                    usedFields = checkUsed(usedFields, MONTH, template);
                    part = Field.MM;
                }
                break;
            case 'D':
                s.readChar('D');
                if (s.readCharIf('D')) {
                    usedFields = checkUsed(usedFields, DAY_OF_YEAR, template);
                    part = Field.DDD;
                } else {
                    usedFields = checkUsed(usedFields, DAY_OF_MONTH, template);
                    part = Field.DD;
                }
                break;
            case 'H':
                s.readChar('H');
                if (s.readCharIf('2')) {
                    s.readChar('4');
                    usedFields = checkUsed(usedFields, HOUR24, template);
                    part = Field.HH24;
                } else {
                    if (s.readCharIf('1')) {
                        s.readChar('2');
                    }
                    usedFields = checkUsed(usedFields, HOUR12, template);
                    part = Field.HH12;
                }
                break;
            case 'S':
                s.readChar('S');
                if (s.readCharIf('S')) {
                    s.readChar('S');
                    s.readChar('S');
                    usedFields = checkUsed(usedFields, SECOND_OF_DAY, template);
                    part = Field.SSSSS;
                } else {
                    usedFields = checkUsed(usedFields, SECOND_OF_MINUTE, template);
                    part = Field.SS;
                }
                break;
            case 'F':
                s.readChar('F');
                c = s.readChar();
                if (c < '1' || c > '9') {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, template);
                }
                usedFields = checkUsed(usedFields, FRACTION, template);
                part = Field.ff(c - '0');
                break;
            case 'A':
            case 'P':
                s.readChar('.');
                s.readChar('M');
                s.readChar('.');
                usedFields = checkUsed(usedFields, AMPM, template);
                part = Field.AM_PM;
                break;
            case 'T':
                s.readChar('Z');
                if (s.readCharIf('H')) {
                    usedFields = checkUsed(usedFields, TIME_ZONE_HOUR, template);
                    part = Field.TZH;
                } else if (s.readCharIf('M')) {
                    usedFields = checkUsed(usedFields, TIME_ZONE_MINUTE, template);
                    part = Field.TZM;
                } else {
                    s.readChar('S');
                    usedFields = checkUsed(usedFields, TIME_ZONE_SECOND, template);
                    part = Field.TZS;
                }
                break;
            default:
                throw DbException.get(ErrorCode.PARSE_ERROR_1, template);
            }
            parts.add(part);
        }
        if (((usedFields & (1 << DAY_OF_YEAR)) != 0 //
                && (usedFields & (1 << MONTH | 1 << DAY_OF_MONTH)) != 0)

                || (((usedFields & (1 << HOUR12)) != 0) //
                        != ((usedFields & (1 << AMPM)) != 0))

                || ((usedFields & (1 << HOUR24)) != 0 //
                        && (usedFields & (1 << HOUR12)) != 0)

                || ((usedFields & (1 << SECOND_OF_DAY)) != 0 //
                        && ((usedFields & (1 << HOUR12 | 1 << HOUR24 | 1 << MINUTE | 1 << SECOND_OF_MINUTE)) != 0))

                || ((usedFields & (1 << TIME_ZONE_SECOND)) != 0 //
                        && !((usedFields & (1 << TIME_ZONE_MINUTE)) != 0))

                || ((usedFields & (1 << TIME_ZONE_MINUTE)) != 0 //
                        && !((usedFields & (1 << TIME_ZONE_HOUR)) != 0))) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, template);
        }
        return new DateTimeTemplate(parts.toArray(new Part[0]), //
                (usedFields & (1 << YEAR | 1 << MONTH | 1 << DAY_OF_MONTH | 1 << DAY_OF_YEAR)) != 0,
                (usedFields & (1 << HOUR24 | 1 << HOUR12 | 1 << MINUTE | 1 << SECOND_OF_MINUTE | 1 << SECOND_OF_DAY
                        | 1 << AMPM)) != 0,
                (usedFields & (1 << TIME_ZONE_HOUR | 1 << TIME_ZONE_MINUTE | 1 << TIME_ZONE_SECOND)) != 0);
    }

    private static int checkUsed(int usedFields, int type, String template) {
        int newUsedFields = usedFields | (1 << type);
        if (usedFields == newUsedFields) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, template);
        }
        return newUsedFields;
    }

    private final Part[] parts;

    private final boolean containsDate, containsTime, containsTimeZone;

    private DateTimeTemplate(Part[] parts, boolean containsDate, boolean containsTime, boolean containsTimeZone) {
        this.parts = parts;
        this.containsDate = containsDate;
        this.containsTime = containsTime;
        this.containsTimeZone = containsTimeZone;
    }

    public String format(Value value) {
        long dateValue, nanoOfDay;
        int offsetSeconds;
        switch (value.getValueType()) {
        case Value.NULL:
            return null;
        case Value.DATE:
            if (containsTime || containsTimeZone) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "time or time zone fields with DATE");
            }
            dateValue = ((ValueDate) value).getDateValue();
            nanoOfDay = 0L;
            offsetSeconds = 0;
            break;
        case Value.TIME:
            if (containsDate || containsTimeZone) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "date or time zone fields with TIME");
            }
            dateValue = 0L;
            nanoOfDay = ((ValueTime) value).getNanos();
            offsetSeconds = 0;
            break;
        case Value.TIME_TZ: {
            if (containsDate) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "date fields with TIME WITH TIME ZONE");
            }
            ValueTimeTimeZone vt = (ValueTimeTimeZone) value;
            dateValue = 0L;
            nanoOfDay = vt.getNanos();
            offsetSeconds = vt.getTimeZoneOffsetSeconds();
            break;
        }
        case Value.TIMESTAMP: {
            if (containsTimeZone) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "time zone fields with TIMESTAMP");
            }
            ValueTimestamp vt = (ValueTimestamp) value;
            dateValue = vt.getDateValue();
            nanoOfDay = vt.getTimeNanos();
            offsetSeconds = 0;
            break;
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone vt = (ValueTimestampTimeZone) value;
            dateValue = vt.getDateValue();
            nanoOfDay = vt.getTimeNanos();
            offsetSeconds = vt.getTimeZoneOffsetSeconds();
            break;
        }
        default:
            throw DbException.getUnsupportedException(value.getType().getTraceSQL());
        }
        StringBuilder builder = new StringBuilder();
        for (Part part : parts) {
            part.format(builder, dateValue, nanoOfDay, offsetSeconds);
        }
        return builder.toString();
    }

    public Value parse(String string, TypeInfo targetType, CastDataProvider provider) {
        switch (targetType.getValueType()) {
        case Value.DATE: {
            if (containsTime || containsTimeZone) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "time or time zone fields with DATE");
            }
            int[] yearMonth = yearMonth(provider);
            return ValueDate.fromDateValue(constructDate(parse(string, yearMonth[0]), yearMonth));
        }
        case Value.TIME:
            if (containsDate || containsTimeZone) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "date or time zone fields with TIME");
            }
            return ValueTime.fromNanos(constructTime(parse(string, 0)));
        case Value.TIME_TZ: {
            if (containsDate) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "date fields with TIME WITH TIME ZONE");
            }
            int[] target = parse(string, 0);
            return ValueTimeTimeZone.fromNanos(constructTime(target), constructOffset(target));
        }
        case Value.TIMESTAMP: {
            if (containsTimeZone) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "time zone fields with TIMESTAMP");
            }
            int[] yearMonth = yearMonth(provider);
            int[] target = parse(string, yearMonth[0]);
            return ValueTimestamp.fromDateValueAndNanos(constructDate(target, yearMonth), constructTime(target));
        }
        case Value.TIMESTAMP_TZ: {
            int[] yearMonth = yearMonth(provider);
            int[] target = parse(string, yearMonth[0]);
            return ValueTimestampTimeZone.fromDateValueAndNanos(constructDate(target, yearMonth), //
                    constructTime(target), constructOffset(target));
        }
        default:
            throw DbException.getUnsupportedException(targetType.getTraceSQL());
        }
    }

    private static int[] yearMonth(CastDataProvider provider) {
        long dateValue = provider.currentTimestamp().getDateValue();
        return new int[] { DateTimeUtils.yearFromDateValue(dateValue), DateTimeUtils.monthFromDateValue(dateValue) };
    }

    private int[] parse(String string, int year) {
        int[] target = new int[15];
        Arrays.fill(target, Integer.MIN_VALUE);
        Scanner s = new Scanner(string);
        for (int i = 0, l = parts.length - 1; i <= l; i++) {
            Part part = parts[i];
            part.parse(target, s, //
                    // Left-delimited
                    (i == 0 //
                            || ((1 << part.type()) & (1 << AMPM | 1 << TIME_ZONE_HOUR)) != 0
                            || ((1 << parts[i - 1].type()) & (1 << DELIMITER | 1 << AMPM)) != 0)
                            // Right-delimited
                            && (i == l //
                                    || part.type() == AMPM //
                                    || ((1 << parts[i + 1].type())
                                            & (1 << DELIMITER | 1 << AMPM | 1 << TIME_ZONE_HOUR)) != 0),
                    year);
        }
        return target;
    }

    private static long constructDate(int[] target, int[] yearMonth) {
        int year = target[YEAR];
        if (year == Integer.MIN_VALUE) {
            year = target[ROUNDED_YEAR];
        }
        if (year == Integer.MIN_VALUE) {
            year = yearMonth[0];
        }
        int dayOfYear = target[DAY_OF_YEAR];
        if (dayOfYear != Integer.MIN_VALUE) {
            if (dayOfYear < 1 || dayOfYear > (DateTimeUtils.isLeapYear(year) ? 366 : 365)) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "Day of year " + dayOfYear);
            }
            return DateTimeUtils.dateValueFromAbsoluteDay(DateTimeUtils.absoluteDayFromYear(year) + dayOfYear - 1);
        }
        int month = target[MONTH];
        if (month == Integer.MIN_VALUE) {
            month = yearMonth[1];
        }
        int day = target[DAY_OF_MONTH];
        if (day == Integer.MIN_VALUE) {
            day = 1;
        }
        if (!DateTimeUtils.isValidDate(year, month, day)) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1,
                    "Invalid date, year=" + year + ", month=" + month + ", day=" + day);
        }
        return DateTimeUtils.dateValue(year, month, day);
    }

    private static long constructTime(int[] target) {
        int secondOfDay = target[SECOND_OF_DAY];
        if (secondOfDay == Integer.MIN_VALUE) {
            int hour = target[HOUR24];
            if (hour == Integer.MIN_VALUE) {
                hour = target[HOUR12];
                if (hour == Integer.MIN_VALUE) {
                    hour = 0;
                } else {
                    if (hour < 1 || hour > 12) {
                        throw DbException.get(ErrorCode.PARSE_ERROR_1, "Hour(12) " + hour);
                    }
                    if (hour == 12) {
                        hour = 0;
                    }
                    hour += target[AMPM] * 12;
                }
            } else {
                if (hour < 0 || hour > 23) {
                    throw DbException.get(ErrorCode.PARSE_ERROR_1, "Hour(24) " + hour);
                }
            }
            int minute = target[MINUTE];
            if (minute == Integer.MIN_VALUE) {
                minute = 0;
            } else if (minute < 0 || minute > 59) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "Minute " + minute);
            }
            int second = target[SECOND_OF_MINUTE];
            if (second == Integer.MIN_VALUE) {
                second = 0;
            } else if (second < 0 || second > 59) {
                throw DbException.get(ErrorCode.PARSE_ERROR_1, "Second of minute " + second);
            }
            secondOfDay = (hour * 60 + minute) * 60 + second;
        } else if (secondOfDay < 0 || secondOfDay >= SECONDS_PER_DAY) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, "Second of day " + secondOfDay);
        }
        int fraction = target[FRACTION];
        if (fraction == Integer.MIN_VALUE) {
            fraction = 0;
        }
        return secondOfDay * NANOS_PER_SECOND + fraction;
    }

    private static int constructOffset(int[] target) {
        int hour = target[TIME_ZONE_HOUR];
        if (hour == Integer.MIN_VALUE) {
            return 0;
        }
        boolean negative = hour < 0;
        if (negative) {
            if (hour == -100) {
                hour = 0;
            } else {
                hour = -hour;
            }
        }
        int minute = target[TIME_ZONE_MINUTE];
        if (minute == Integer.MIN_VALUE) {
            minute = 0;
        } else if (minute > 59) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, "Time zone minute " + minute);
        }
        int second = target[TIME_ZONE_SECOND];
        if (second == Integer.MIN_VALUE) {
            second = 0;
        } else if (second > 59) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, "Time zone second " + second);
        }
        int offset = (hour * 60 + minute) * 60 + second;
        if (offset > 18 * 60 * 60) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, "Time zone offset is too large");
        }
        return negative ? -offset : offset;
    }

}
