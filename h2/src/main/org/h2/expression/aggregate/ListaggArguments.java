/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

/**
 * Additional arguments of LISTAGG aggregate function.
 */
public final class ListaggArguments {

    private String separator;

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

}
