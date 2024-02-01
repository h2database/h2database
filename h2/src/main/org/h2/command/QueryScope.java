/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.LinkedHashMap;

import org.h2.table.Table;

/**
 * The scope of identifiers for a query with the WITH clause.
 */
public final class QueryScope {

    /**
     * The scope of a parent query with the WITH clause.
     */
    public final QueryScope parent;

    /**
     * The elements of the WITH list.
     */
    public final LinkedHashMap<String, Table> tableSubqueries;

    /**
     * Creates new instance of a query scope.
     *
     * @param parent
     *            parent scope, or {@code null}
     */
    public QueryScope(QueryScope parent) {
        this.parent = parent;
        tableSubqueries = new LinkedHashMap<>();
    }

}
