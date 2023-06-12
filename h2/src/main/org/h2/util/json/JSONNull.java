/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

/**
 * JSON null.
 */
public final class JSONNull extends JSONValue {

    /**
     * {@code null} value.
     */
    public static final JSONNull NULL = new JSONNull();

    private JSONNull() {
    }

    @Override
    public void addTo(JSONTarget<?> target) {
        target.valueNull();
    }

}
