package org.h2.util;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * Main class
 */
public class ToDate {
    public static Timestamp TO_DATE(final String input, final String format) {
        ToDateParser parser = ToDateParser.toDate(input, format);
        return parser.getResultingTimestamp();
    }
    public static Timestamp TO_TIMESTAMP(final String input, final String format) {
        ToDateParser parser = ToDateParser.toTimestamp(input, format);
        return parser.getResultingTimestamp();
    }
}