/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.util.ArrayList;

/**
 * JSON array.
 */
public class JSONArray extends JSONValue {

    private final ArrayList<JSONValue> elements = new ArrayList<>();

    JSONArray() {
    }

    /**
     * Add a value to the array.
     *
     * @param value the value to add
     */
    void addElement(JSONValue value) {
        elements.add(value);
    }

    @Override
    public void addTo(JSONTarget<?> target) {
        target.startArray();
        for (JSONValue element : elements) {
            element.addTo(target);
        }
        target.endArray();
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public JSONValue[] getArray() {
        return elements.toArray(new JSONValue[0]);
    }

}
