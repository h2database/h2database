/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.LinkedHashMap;

import org.h2.table.Table;

public final class QueryScope {

    public final QueryScope parent;

    public final LinkedHashMap<String, Table> tableSubqeries;

    public QueryScope(QueryScope parent) {
        this.parent = parent;
        tableSubqeries = new LinkedHashMap<>();
    }

}
