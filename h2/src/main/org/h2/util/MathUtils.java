/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * This is a utility class with mathematical helper functions.
 */
public class MathUtils {
    // with blockSizePowerOf2 8: 0 > 0; 1..8 > 8, 9..16 > 16, ...
    public static int roundUp(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    public static long roundUpLong(long x, long blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    public static void checkPowerOf2(int len) {
        if ((len & (len - 1)) != 0 && len > 0) {
            throw Message.getInternalError("not a power of 2: " + len);
        }
    }

    public static int nextPowerOf2(int x) {
        long i = 1;
        while (i < x && i < (Integer.MAX_VALUE / 2)) {
            i += i;
        }
        return (int) i;
    }

    public static long scaleUp50Percent(long start, long min, long blockSize, long maxIncrease) {
        long len;
        if (min > maxIncrease * 2) {
            len = MathUtils.roundUpLong(min, maxIncrease);
        } else {
            len = start;
            while (len < min) {
                len += len / 2;
                len += len % blockSize;
            }
        }
        return len;
    }

    public static BigDecimal setScale(BigDecimal bd, int scale) throws SQLException {
        if (scale > Constants.BIG_DECIMAL_SCALE_MAX) {
            throw Message.getInvalidValueException("" + scale, "scale");
        } else if (scale < 0) {
            throw Message.getInvalidValueException("" + scale, "scale");
        }
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
    }

    public static byte decodeByte(String s) {
        return Byte.decode(s).byteValue();
    }

    public static short decodeShort(String s) {
        return Short.decode(s).shortValue();
    }

    public static int decodeInt(String s) {
        return Integer.decode(s).intValue();
    }

    public static long decodeLong(String s) {
        return Long.decode(s).longValue();
    }

    public static int convertLongToInt(long l) {
        if (l <= Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if (l >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) l;
        }
    }

}
