/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.value.ValueTimestampTimeZone;

public final class CurrentTimestamp {

    /*
     * Signatures of methods should match with
     * h2/src/java9/src/org/h2/util/CurrentTimestamp.java and precompiled
     * h2/src/java9/precompiled/org/h2/util/CurrentTimestamp.class.
     */

    /**
     * Returns current timestamp.
     *
     * @return current timestamp
     */
    public static ValueTimestampTimeZone get() {
        long ms = System.currentTimeMillis();
        /*
         * This code intentionally does not support properly dates before UNIX
         * epoch and time zone offsets with seconds because such support is not
         * required for current dates.
         */
        int offset = DateTimeUtils.getTimeZone().getOffset(ms);
        ms += offset;
        return ValueTimestampTimeZone.fromDateValueAndNanos(
                DateTimeUtils.dateValueFromAbsoluteDay(ms / DateTimeUtils.MILLIS_PER_DAY),
                ms % DateTimeUtils.MILLIS_PER_DAY * 1_000_000, (short) (offset / 60_000));
    }

    private CurrentTimestamp() {
    }

}
