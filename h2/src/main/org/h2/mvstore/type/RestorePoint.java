/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Enno Thieleke
 */
package org.h2.mvstore.type;

import org.h2.value.ValueBigint;
import org.h2.value.ValueTimestampTimeZone;

public class RestorePoint implements Comparable<RestorePoint> {

    private final String name;
    private final ValueTimestampTimeZone createdAt;
    private final ValueBigint oldestDatabaseVersionToKeep, databaseVersion;

    public RestorePoint(
            String name,
            ValueTimestampTimeZone createdAt,
            ValueBigint oldestDatabaseVersionToKeep,
            ValueBigint databaseVersion
    ) {
        this.name = name;
        this.createdAt = createdAt;
        this.oldestDatabaseVersionToKeep = oldestDatabaseVersionToKeep;
        this.databaseVersion = databaseVersion;
    }

    public String getName() {
        return name;
    }

    public ValueTimestampTimeZone getCreatedAt() {
        return createdAt;
    }

    public ValueBigint getOldestDatabaseVersionToKeep() {
        return oldestDatabaseVersionToKeep;
    }

    public ValueBigint getDatabaseVersion() {
        return databaseVersion;
    }

    @Override
    public int compareTo(RestorePoint o) {
        // Only the database version is relevant, because within a single database it's not possible
        // to have multiple restore points that reference the same version, because each restore point
        // results in a new MVStore version.
        return Long.compare(databaseVersion.getLong(), o.databaseVersion.getLong());
    }
}
