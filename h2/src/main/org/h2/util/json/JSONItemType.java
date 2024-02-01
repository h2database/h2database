/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

/**
 * JSON item type.
 */
public enum JSONItemType {

    /**
     * Either {@link #ARRAY}, {@link #OBJECT}, or {@link #SCALAR}.
     */
    VALUE,

    /**
     * JSON array.
     */
    ARRAY,

    /**
     * JSON object.
     */
    OBJECT,

    /**
     * JSON scalar value: string, number, {@code true}, {@code false}, or
     * {@code null}.
     */
    SCALAR;

    /**
     * Checks whether this item type includes the specified item type.
     *
     * @param type
     *            item type to check
     * @return whether this item type includes the specified item type
     */
    public boolean includes(JSONItemType type) {
        if (type == null) {
            throw new NullPointerException();
        }
        return this == VALUE || this == type;
    }

}
