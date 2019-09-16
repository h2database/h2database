/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.value.ValueTimestampTimeZone;

/**
 * Provides information for type casts and comparison operations.
 */
public interface CastDataProvider {

    /**
     * Returns the current timestamp with maximum resolution. The value must be
     * the same within a transaction or within execution of a command.
     *
     * @return the current timestamp for CURRENT_TIMESTAMP(9)
     */
    ValueTimestampTimeZone currentTimestamp();

    /**
     * Returns the database mode.
     *
     * @return the database mode
     */
    Mode getMode();

}
