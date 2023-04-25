/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.expression.Expression;

/**
 * Additional arguments of LISTAGG aggregate function.
 */
public final class ListaggArguments {

    private Expression separator;

    private boolean onOverflowTruncate;

    private Expression filter;

    private boolean withoutCount;

    /**
     * Creates a new instance of additional arguments of LISTAGG aggregate
     * function.
     */
    public ListaggArguments() {
    }

    /**
     * Sets the custom LISTAGG separator.
     *
     * @param separator
     *            the LISTAGG separator, {@code null} or empty string means no
     *            separator
     */
    public void setSeparator(Expression separator) {
        this.separator = separator;
    }

    /**
     * Returns the LISTAGG separator.
     *
     * @return the LISTAGG separator, {@code null} means the default
     */
    public Expression getSeparator() {
        return separator;
    }

    /**
     * Returns the effective LISTAGG separator.
     *
     * @return the effective LISTAGG separator
     */
    public String getEffectiveSeparator() {
        if (separator != null) {
            String s = separator.getValue(null).getString();
            return s != null ? s : "";
        }
        return ",";
    }

    /**
     * Sets the LISTAGG overflow behavior.
     *
     * @param onOverflowTruncate
     *            {@code true} for ON OVERFLOW TRUNCATE, {@code false} for ON
     *            OVERFLOW ERROR
     */
    public void setOnOverflowTruncate(boolean onOverflowTruncate) {
        this.onOverflowTruncate = onOverflowTruncate;
    }

    /**
     * Returns the LISTAGG overflow behavior.
     *
     * @return {@code true} for ON OVERFLOW TRUNCATE, {@code false} for ON
     *         OVERFLOW ERROR
     */
    public boolean getOnOverflowTruncate() {
        return onOverflowTruncate;
    }

    /**
     * Sets the custom LISTAGG truncation filter.
     *
     * @param filter
     *            the LISTAGG truncation filter, {@code null} or empty string
     *            means no truncation filter
     */
    public void setFilter(Expression filter) {
        this.filter = filter;
    }

    /**
     * Returns the LISTAGG truncation filter.
     *
     * @return the LISTAGG truncation filter, {@code null} means the default
     */
    public Expression getFilter() {
        return filter;
    }

    /**
     * Returns the effective LISTAGG truncation filter.
     *
     * @return the effective LISTAGG truncation filter
     */
    public String getEffectiveFilter() {
        if (filter != null) {
            String f = filter.getValue(null).getString();
            return f != null ? f : "";
        }
        return "...";
    }

    /**
     * Sets the LISTAGG count indication.
     *
     * @param withoutCount
     *            {@code true} for WITHOUT COUNT, {@code false} for WITH COUNT
     */
    public void setWithoutCount(boolean withoutCount) {
        this.withoutCount = withoutCount;
    }

    /**
     * Returns the LISTAGG count indication.
     *
     * @return {@code true} for WITHOUT COUNT, {@code false} for WITH COUNT
     */
    public boolean isWithoutCount() {
        return withoutCount;
    }

}
