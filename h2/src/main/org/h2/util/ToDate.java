package org.h2.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.h2.util.ToDateTokenizer.FormatTokenEnum;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * Main class
 */
public class ToDate {
    public enum TO_DATE_FunctionName {
        TO_DATE("DD MON YYYY"), TO_TIMESTAMP("DD MON YYYY HH:MI:SS");

        private final String defaultFormatStr;

        TO_DATE_FunctionName(final String defaultFormatStr) {
            this.defaultFormatStr = defaultFormatStr;
        }

        String getDefaultFormatStr() {
            return defaultFormatStr;
        }
    };

    public static Date TO_DATE(final String input, final String format) {
        ToDateParams parsed = parse(new ToDateParams(TO_DATE_FunctionName.TO_DATE, input, format));
        return parsed.getResultingDate();
    }

    public static Timestamp TO_TIMESTAMP(final String input, final String format) {
        ToDateParams parsed = parse(new ToDateParams(TO_DATE_FunctionName.TO_TIMESTAMP, input, format));
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
