/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import static org.h2.result.SortOrder.DESCENDING;
import static org.h2.result.SortOrder.NULLS_FIRST;
import static org.h2.result.SortOrder.NULLS_LAST;

/**
 * Default ordering of NULL values.
 */
public enum DefaultNullOrdering {

    /**
     * NULL values are considered as smaller than other values during sorting.
     */
    LOW(NULLS_FIRST, NULLS_LAST),

    /**
     * NULL values are considered as larger than other values during sorting.
     */
    HIGH(NULLS_LAST, NULLS_FIRST),

    /**
     * NULL values are sorted before other values, no matter if ascending or
     * descending order is used.
     */
    FIRST(NULLS_FIRST, NULLS_FIRST),

    /**
     * NULL values are sorted after other values, no matter if ascending or
     * descending order is used.
     */
    LAST(NULLS_LAST, NULLS_LAST);

    private static final DefaultNullOrdering[] VALUES = values();

    /**
     * Returns default ordering of NULL values for the specified ordinal number.
     *
     * @param ordinal
     *            ordinal number
     * @return default ordering of NULL values for the specified ordinal number
     * @see #ordinal()
     */
    public static DefaultNullOrdering valueOf(int ordinal) {
        return VALUES[ordinal];
    }

    private final int defaultAscNulls, defaultDescNulls;

    private final int nullAsc, nullDesc;

    private DefaultNullOrdering(int defaultAscNulls, int defaultDescNulls) {
        this.defaultAscNulls = defaultAscNulls;
        this.defaultDescNulls = defaultDescNulls;
        nullAsc = defaultAscNulls == NULLS_FIRST ? -1 : 1;
        nullDesc = defaultDescNulls == NULLS_FIRST ? -1 : 1;
    }

    /**
     * Returns a sort type bit mask with {@link org.h2.result.SortOrder#NULLS_FIRST} or
     * {@link org.h2.result.SortOrder#NULLS_LAST} explicitly set
     *
     * @param sortType
     *            sort type bit mask
     * @return bit mask with {@link org.h2.result.SortOrder#NULLS_FIRST} or {@link org.h2.result.SortOrder#NULLS_LAST}
     *         explicitly set
     */
    public int addExplicitNullOrdering(int sortType) {
        if ((sortType & (NULLS_FIRST | NULLS_LAST)) == 0) {
            sortType |= ((sortType & DESCENDING) == 0 ? defaultAscNulls : defaultDescNulls);
        }
        return sortType;
    }

    /**
     * Compare two expressions where one of them is NULL.
     *
     * @param aNull
     *            whether the first expression is null
     * @param sortType
     *            the sort bit mask to use
     * @return the result of the comparison (-1 meaning the first expression
     *         should appear before the second, 0 if they are equal)
     */
    public int compareNull(boolean aNull, int sortType) {
        if ((sortType & NULLS_FIRST) != 0) {
            return aNull ? -1 : 1;
        } else if ((sortType & NULLS_LAST) != 0) {
            return aNull ? 1 : -1;
        } else if ((sortType & DESCENDING) == 0) {
            return aNull ? nullAsc : -nullAsc;
        } else {
            return aNull ? nullDesc : -nullDesc;
        }
    }

}
