/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.index.Index;

import java.util.Set;

/**
 * Contains the hints for which index to use for a specific table. Currently
 * allows a list of "use indexes" to be specified.
 * <p>
 * Use the factory method IndexHints.createUseIndexHints(listOfIndexes) to limit
 * the query planner to only use specific indexes when determining which index
 * to use for a table
 * <p>
 * Currently supports "USE INDEX" syntax.
 **/
public final class IndexHints {

    private final boolean useOnlySpecifiedIndexes;
    private final Set<String> useIndexList;

    private IndexHints(boolean useOnlySpecifiedIndexes, Set<String> useIndexList) {
        this.useOnlySpecifiedIndexes = useOnlySpecifiedIndexes;
        this.useIndexList = useIndexList;
    }

    public static IndexHints createUseIndexHints(Set<String> useIndexList) {
        return new IndexHints(true, useIndexList);
    }

    public Set<String> getUseIndexList() {
        return useIndexList;
    }

    @Override
    public String toString() {
        return "IndexHints{" +
                "useOnlySpecifiedIndexes=" + useOnlySpecifiedIndexes +
                ", useIndexList=" + useIndexList +
                '}';
    }

    public boolean allowIndex(Index index) {
        return !useOnlySpecifiedIndexes || useIndexList.contains(index.getName());
    }
}
