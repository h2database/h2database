/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

/**
 * The settings of one full text search index.
 */
class IndexInfo {

    /**
     * The index id.
     */
    int id;

    /**
     * The schema name.
     */
    String schema;

    /**
     * The table name.
     */
    String table;

    /**
     * The column indexes of the key columns.
     */
    int[] keys;

    /**
     * The column indexes of the index columns.
     */
    int[] indexColumns;

    /**
     * The column names.
     */
    String[] columns;
}
