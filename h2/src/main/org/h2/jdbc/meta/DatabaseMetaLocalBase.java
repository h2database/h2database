/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc.meta;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;

/**
 * Base implementation of database meta information.
 */
abstract class DatabaseMetaLocalBase extends DatabaseMeta {

    @Override
    public final boolean nullsAreSortedHigh() {
        return SysProperties.SORT_NULLS_HIGH;
    }

    @Override
    public final String getDatabaseProductVersion() {
        return Constants.FULL_VERSION;
    }

    @Override
    public final int getDatabaseMajorVersion() {
        return Constants.VERSION_MAJOR;
    }

    @Override
    public final int getDatabaseMinorVersion() {
        return Constants.VERSION_MINOR;
    }

}
