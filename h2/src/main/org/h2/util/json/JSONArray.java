/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * JSON array.
 */
public final class JSONArray extends JSONValue {

    private final ArrayList<JSONValue> elements = new ArrayList<>();

    JSONArray() {
    }

    /**
     * Add a value to the array.
     *
     * @param value
     *            the value to add
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
     * Returns the array length
     *
     * @return the array length
     */
    public int length() {
        return elements.size();
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public JSONValue[] getArray() {
        return elements.toArray(new JSONValue[0]);
    }

    /**
     * Returns the value.
     *
     * @param elementType
     *            the type of array elements
     * @param converter
     *            a converter to the specified type
     * @param <E>
     *            type of elements
     * @return the value
     */
    public <E> E[] getArray(Class<E> elementType, Function<JSONValue, E> converter) {
        int length = elements.size();
        @SuppressWarnings("unchecked")
        E[] array = (E[]) Array.newInstance(elementType, length);
        for (int i = 0; i < length; i++) {
            array[i] = converter.apply(elements.get(i));
        }
        return array;
    }

    /**
     * Returns the value at specified 0-based index, or {@code null}.
     *
     * @param index
     *            0-based index
     * @return the value at specified 0-based index, or {@code null}.
     */
    public JSONValue getElement(int index) {
        if (index >= 0 && index < elements.size()) {
            return elements.get(index);
        }
        return null;
    }

}
