/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

/**
 * JSON string.
 */
public final class JSONString extends JSONValue {

    private final String value;

    JSONString(String value) {
        this.value = value;
    }

    @Override
    public void addTo(JSONTarget<?> target) {
        target.valueString(value);
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public String getString() {
        return value;
    }

}
