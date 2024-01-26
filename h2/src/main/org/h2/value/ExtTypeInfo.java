/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.util.HasSQL;

/**
 * Extended parameters of a data type.
 */
public abstract class ExtTypeInfo implements HasSQL {

    @Override
    public String toString() {
        return getSQL(QUOTE_ONLY_WHEN_REQUIRED);
    }

}
