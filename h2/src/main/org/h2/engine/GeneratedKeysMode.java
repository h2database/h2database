/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;

/**
 * Modes of generated keys' gathering.
 */
public final class GeneratedKeysMode {

    /**
     * Generated keys are not needed.
     */
    public static final int NONE = 0;

    /**
     * Generated keys should be configured automatically.
     */
    public static final int AUTO = 1;

    /**
     * Use specified column indices to return generated keys from.
     */
    public static final int COLUMN_NUMBERS = 2;

    /**
     * Use specified column names to return generated keys from.
     */
    public static final int COLUMN_NAMES = 3;

    /**
     * Determines mode of generated keys' gathering.
     *
     * @param generatedKeysRequest
     *            {@code null} or {@code false} if generated keys are not
     *            needed, {@code true} if generated keys should be configured
     *            automatically, {@code int[]} to specify column indices to
     *            return generated keys from, or {@code String[]} to specify
     *            column names to return generated keys from
     * @return mode for the specified generated keys request
     */
    public static int valueOf(Object generatedKeysRequest) {
        if (generatedKeysRequest == null || Boolean.FALSE.equals(generatedKeysRequest)) {
            return NONE;
        }
        if (Boolean.TRUE.equals(generatedKeysRequest)) {
            return AUTO;
        }
        if (generatedKeysRequest instanceof int[]) {
            return ((int[]) generatedKeysRequest).length > 0 ? COLUMN_NUMBERS : NONE;
        }
        if (generatedKeysRequest instanceof String[]) {
            return ((String[]) generatedKeysRequest).length > 0 ? COLUMN_NAMES : NONE;
        }
        throw DbException.getInternalError();
    }

    private GeneratedKeysMode() {
    }

}
