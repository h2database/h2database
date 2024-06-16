/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

/**
 * Index-sorting information.
 */
public final class IndexSort implements Comparable<IndexSort> {

    /**
     * Used instead of actual number of sorted columns when post-sorting isn't
     * needed.
     */
    public static final int FULLY_SORTED = Integer.MAX_VALUE;

    private final Index index;

    /**
     * A positive number of sorted columns for partial index sorts, or
     * {@link #FULLY_SORTED} for complete index sorts.
     */
    private final int sortedColumns;

    /**
     * Whether index must be iterated in reverse order.
     */
    private final boolean reverse;

    /**
     * Creates an index sorting information for complete index sort.
     *
     * @param index
     *            the index
     * @param reverse
     *            whether index must be iterated in reverse order
     */
    public IndexSort(Index index, boolean reverse) {
        this(index, FULLY_SORTED, reverse);
    }

    /**
     * Creates an index sorting information for index sort.
     *
     * @param index
     *            the index
     * @param sortedColumns
     *            a positive number of sorted columns for partial index sorts,
     *            or {@link #FULLY_SORTED} for complete index sorts
     * @param reverse
     *            whether index must be iterated in reverse order
     */
    public IndexSort(Index index, int sortedColumns, boolean reverse) {
        this.index = index;
        this.sortedColumns = sortedColumns;
        this.reverse = reverse;
    }

    /**
     * Returns the index.
     *
     * @return the index
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns number of sorted columns.
     *
     * @return positive number of sorted columns for partial index sorts, or
     *         {@link #FULLY_SORTED} for complete index sorts
     */
    public int getSortedColumns() {
        return sortedColumns;
    }

    /**
     * Returns whether index must be iterated in reverse order.
     *
     * @return {@code true} for reverse order, {@code false} for natural order
     */
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public int compareTo(IndexSort o) {
        return o.sortedColumns - sortedColumns;
    }

}
