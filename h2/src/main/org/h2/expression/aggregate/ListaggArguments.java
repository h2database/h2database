/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

/**
 * Additional arguments of LISTAGG aggregate function.
 */
public final class ListaggArguments {

    private String separator;

    private boolean onOverflowTruncate;

    private String filter;

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
    public void setSeparator(String separator) {
        this.separator = separator != null ? separator : "";
    }

    /**
     * Returns the LISTAGG separator.
     *
     * @return the LISTAGG separator, {@code null} means the default
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * Returns the effective LISTAGG separator.
     *
     * @return the effective LISTAGG separator
     */
    public String getEffectiveSeparator() {
        return separator != null ? separator : ",";
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
    public void setFilter(String filter) {
        this.filter = filter != null ? filter : "";
    }

    /**
     * Returns the LISTAGG truncation filter.
     *
     * @return the LISTAGG truncation filter, {@code null} means the default
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Returns the effective LISTAGG truncation filter.
     *
     * @return the effective LISTAGG truncation filter
     */
    public String getEffectiveFilter() {
        return filter != null ? filter : "...";
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
