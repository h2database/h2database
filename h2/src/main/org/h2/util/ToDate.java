package org.h2.util;

import java.sql.Timestamp;
import java.util.List;

import org.h2.util.ToDateTokenizer.FormatTokenEnum;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * Main class
 */
public class ToDate {
    public enum ToDateFunctionName {
        TO_DATE("DD MON YYYY"), TO_TIMESTAMP("DD MON YYYY HH:MI:SS");

        private final String defaultFormatStr;

        ToDateFunctionName(final String defaultFormatStr) {
            this.defaultFormatStr = defaultFormatStr;
        }

        String getDefaultFormatStr() {
            return defaultFormatStr;
        }
    };

    public static Timestamp TO_DATE(final String input, final String format) {
        ToDateParams parsed = parse(new ToDateParams(ToDateFunctionName.TO_DATE, input, format));
        return parsed.getResultingTimestamp();
    }

    public static Timestamp TO_TIMESTAMP(final String input, final String format) {
        ToDateParams parsed = parse(new ToDateParams(ToDateFunctionName.TO_TIMESTAMP, input, format));
        return parsed.getResultingTimestamp();
    }

    /**
     * Parse the format-string with passed token of {@link FormatTokenEnum}}.<br>
     * if token matches return true otherwise false.
     */
    private static ToDateParams parse(final ToDateParams p) {
        while (p.hasToParseData()) {
            List<FormatTokenEnum> tokenList = FormatTokenEnum.getTokensInQuestion(p);
            if (tokenList.isEmpty()) {
                p.removeFirstChar();
                continue;
            }
            boolean foundAnToken = false;
            for (FormatTokenEnum token : tokenList) {
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

}