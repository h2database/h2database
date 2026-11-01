/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

final class FuzzParseUtil {

    private FuzzParseUtil() {}

    static String kv(String line, String key) {
        String prefix = key + "=";
        for (String part : line.split(" ")) {
            if (part.startsWith(prefix)) {
                return part.substring(prefix.length());
            }
        }
        throw new IllegalArgumentException("Missing key '" + key + "' in: " + line);
    }

    static String kvOpt(String line, String key, String defaultValue) {
        String prefix = key + "=";
        for (String part : line.split(" ")) {
            if (part.startsWith(prefix)) {
                return part.substring(prefix.length());
            }
        }
        return defaultValue;
    }
}
