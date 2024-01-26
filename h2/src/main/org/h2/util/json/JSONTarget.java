/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.math.BigDecimal;

/**
 * Abstract JSON output target.
 *
 * @param <R>
 *            the type of the result
 */
public abstract class JSONTarget<R> {

    /**
     * Start of an object.
     */
    public abstract void startObject();

    /**
     * End of the current object.
     */
    public abstract void endObject();

    /**
     * Start of an array.
     */
    public abstract void startArray();

    /**
     * End of the current array.
     */
    public abstract void endArray();

    /**
     * Name of a member.
     *
     * @param name
     *            the name
     */
    public abstract void member(String name);

    /**
     * Parse "null".
     *
     * {@code null} value.
     */
    public abstract void valueNull();

    /**
     * Parse "false".
     *
     * {@code false} value.
     */
    public abstract void valueFalse();

    /**
     * Parse "true".
     *
     * {@code true} value.
     */
    public abstract void valueTrue();

    /**
     * A number value.
     *
     * @param number
     *            the number
     */
    public abstract void valueNumber(BigDecimal number);

    /**
     * A string value.
     *
     * @param string
     *            the string
     */
    public abstract void valueString(String string);

    /**
     * Returns whether member's name or the end of the current object is
     * expected.
     *
     * @return {@code true} if it is, {@code false} otherwise
     */
    public abstract boolean isPropertyExpected();

    /**
     * Returns whether value separator expected before the next member or value.
     *
     * @return {@code true} if it is, {@code false} otherwise
     */
    public abstract boolean isValueSeparatorExpected();

    /**
     * Returns the result.
     *
     * @return the result
     */
    public abstract R getResult();

}
