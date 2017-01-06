package org.h2.table;

import java.util.Collections;
import java.util.List;

/**
 * Contains the hints for which index to use for a specific table. Currently allows no hints to be specified, or
 * a list of "use indexes" to be specified.
 * <p>
 * Use IndexHints.NONE for the most common case of table having no index hints. This instance can be safely reused.
 * <p>
 * Use factory method IndexHints.createUseIndexHints(listOfIndexes) to limit the query planner to only use
 * specific indexes when determining which index to use for a table
 * <p>
 * Currently handles "USE INDEX" syntax only, but allows adding of other syntax such as "IGNORE INDEX" and "FORCE INDEX".
 **/
public final class IndexHints {

    public static final IndexHints NONE = new IndexHints();

    private final boolean useOnlySpecifiedIndexes;
    private final List<String> useIndexList;

    private IndexHints() {
        this(false, Collections.<String>emptyList());
    }

    private IndexHints(boolean useOnlySpecifiedIndexes, List<String> useIndexList) {
        this.useOnlySpecifiedIndexes = useOnlySpecifiedIndexes;
        this.useIndexList = useIndexList;
    }

    public boolean isUseOnlySpecifiedIndexes() {
        return useOnlySpecifiedIndexes;
    }

    public List<String> getUseIndexList() {
        return useIndexList;
    }

    public static IndexHints createUseIndexHints(List<String> useIndexList) {
        return new IndexHints(true, useIndexList);
    }

    @Override
    public String toString() {
        return "IndexHints{" +
                "useOnlySpecifiedIndexes=" + useOnlySpecifiedIndexes +
                ", useIndexList=" + useIndexList +
                '}';
    }
}
