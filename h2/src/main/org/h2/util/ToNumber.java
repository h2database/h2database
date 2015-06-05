package org.h2.util;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Converts String into BigDecimal<br>
 * A TO_NUMBER in "Best Effort" manner that will cover 99% of the use cases.<br>
 * To be fully Oracle compatible would cost 10x more coding effort for the 1%
 * case.<br>
 * Following input will work:
 * 
 * <pre>
 *   "$123"                  -> 123
 *   "Fr 123.0"              -> 123.0
 *   "foo  1,234.56e+1  bar" -> 12345.6
 *   " 1 2 3 . 4"            -> 123.4
 * </pre>
 * 
 * NOTE: It's assumed that DecimalFormatSymbol is '.'. Separator chars like
 * space, dash are ignored.<br>
 * TODO: Missing HEX and OCTAL number handling
 */
public final class ToNumber {

    private static Pattern P_NonNumberPrefix = Pattern.compile("([^0-9\\.\\-\\+]*)(.*)");
    private static Pattern P_AnyNumber = Pattern.compile("([0-9\\.\\-\\+\\'\\, ]*[eE]*[ 0-9\\-\\+]*).*");

    public static BigDecimal toNumber(String numberStr, String format, String nlsParam) {
        BigDecimal result = null;
        String detectedNumberStr = "";
        if (numberStr == null) {
            // ignore
        } else {
            detectedNumberStr = numberStr.trim();
            Matcher m = P_NonNumberPrefix.matcher(detectedNumberStr);
            if (m.matches()) {
                detectedNumberStr = m.group(2);
            }
            m = P_AnyNumber.matcher(detectedNumberStr);
            if (m.matches()) {
                detectedNumberStr = m.group(1);
            }
            detectedNumberStr = detectedNumberStr.replace(" ", "").replace("'", "").replace(",", "");
            try {
                result = new BigDecimal(detectedNumberStr);
            } catch (RuntimeException e) {
                // ignore
            }
        }
        if (result == null) {
            throw DbException.get(ErrorCode.INVALID_TO_NUMBER_FORMAT,
                    String.format("NumString: '%s', FormatStr: '%s', Detected NumberStr: '%s'", //
                            numberStr, format, detectedNumberStr));
        }
        return result;
    }

}
