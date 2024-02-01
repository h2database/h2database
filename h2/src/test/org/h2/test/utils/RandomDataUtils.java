/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.util.Random;

/**
 * Utilities for random data generation.
 */
public final class RandomDataUtils {

    /**
     * Fills the specified character array with random printable code points
     * from the limited set of Unicode code points with different length in
     * UTF-8 representation.
     *
     * <p>
     * Debuggers can have performance problems on some systems when displayed
     * values have characters from many different blocks, because too many large
     * separate fonts with different sets of glyphs can be needed.
     * </p>
     *
     * @param r
     *            the source of random data
     * @param chars
     *            the character array to fill
     */
    public static void randomChars(Random r, char[] chars) {
        for (int i = 0, l = chars.length; i < l;) {
            int from, to;
            switch (r.nextInt(4)) {
            case 3:
                if (i + 1 < l) {
                    from = 0x1F030;
                    to = 0x1F093;
                    break;
                }
                //$FALL-THROUGH$
            default:
                from = ' ';
                to = '~';
                break;
            case 1:
                from = 0xA0;
                to = 0x24F;
                break;
            case 2:
                from = 0x2800;
                to = 0x28FF;
                break;
            }
            i += Character.toChars(from + r.nextInt(to - from + 1), chars, i);
        }
    }

    private RandomDataUtils() {
    }

}
