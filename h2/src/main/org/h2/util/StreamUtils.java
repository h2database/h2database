/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.util;

import java.util.function.Predicate;

public abstract class StreamUtils {
    private StreamUtils() {
    }

    /** Negates a predicate for example used in {@link java.util.stream.Stream#filter} */
    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }
}
