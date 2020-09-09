/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * Page Store implementation of a row.
 */
public final class PageStoreRow {

    /**
     * An empty array of Row objects.
     */
    static final Row[] EMPTY_ARRAY = new Row[0];

    /**
     * An empty array of SearchRow objects.
     */
    static final SearchRow[] EMPTY_SEARCH_ARRAY = new SearchRow[0];

    /**
     * The implementation of a removed row in an in-memory table.
     */
    static final class RemovedRow extends Row {

        RemovedRow(long key) {
            setKey(key);
        }

        @Override
        public Value getValue(int i) {
            if (i == ROWID_INDEX) {
                return ValueBigint.get(key);
            }
            throw DbException.getInternalError();
        }

        @Override
        public void setValue(int i, Value v) {
            if (i == ROWID_INDEX) {
                key = v.getLong();
            } else {
                throw DbException.getInternalError();
            }
        }

        @Override
        public int getColumnCount() {
            return 0;
        }

        @Override
        public String toString() {
            return "( /* key:" + key + " */ )";
        }

        @Override
        public int getMemory() {
            return Constants.MEMORY_ROW;
        }

        @Override
        public Value[] getValueList() {
            return null;
        }

        @Override
        public void copyFrom(SearchRow source) {
            setKey(source.getKey());
        }
    }

    private PageStoreRow() {
    }

}
