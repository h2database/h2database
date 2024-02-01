/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * An object with data type.
 */
public interface Typed {

    /**
     * Returns the data type.
     *
     * @return the data type
     */
    TypeInfo getType();

}
