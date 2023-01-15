/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.CastDataProvider;
import org.h2.mvstore.db.RowDataType;
import org.h2.store.DataHandler;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Typed;
import org.h2.value.Value;

/**
 * Creates rows.
 *
 * @author Sergi Vladykin
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class RowFactory {

    private static final class Holder {
        static final RowFactory EFFECTIVE = DefaultRowFactory.INSTANCE;
    }

    public static DefaultRowFactory getDefaultRowFactory() {
        return DefaultRowFactory.INSTANCE;
    }

    public static RowFactory getRowFactory() {
        return Holder.EFFECTIVE;
    }

    /**
     * Create a new row factory.
     *
     * @param provider the cast provider
     * @param compareMode the compare mode
     * @param handler the data handler
     * @param columns the list of columns
     * @param indexColumns the list of index columns
     * @param storeKeys whether row keys are stored
     * @return the (possibly new) row factory
     */
    public RowFactory createRowFactory(CastDataProvider provider, CompareMode compareMode, DataHandler handler,
            Typed[] columns, IndexColumn[] indexColumns, boolean storeKeys) {
        return this;
    }

    /**
     * Create a new row.
     *
     * @param data the values
     * @param memory the estimated memory usage in bytes
     * @return the created row
     */
    public abstract Row createRow(Value[] data, int memory);

    /**
     * Create new row.
     *
     * @return the created row
     */
    public abstract SearchRow createRow();

    public abstract RowDataType getRowDataType();

    public abstract int[] getIndexes();

    public abstract TypeInfo[] getColumnTypes();

    public abstract int getColumnCount();

    public abstract boolean getStoreKeys();


    /**
     * Default implementation of row factory.
     */
    public static final class DefaultRowFactory extends RowFactory {
        private final RowDataType dataType;
        private final int         columnCount;
        private final int[]       indexes;
        private TypeInfo[]        columnTypes;
        private final int[]       map;

        public static final DefaultRowFactory INSTANCE = new DefaultRowFactory();

        DefaultRowFactory() {
            this(new RowDataType(null, CompareMode.getInstance(null, 0), null, null, null, 0, true), 0, null, null);
        }

        private DefaultRowFactory(RowDataType dataType, int columnCount, int[] indexes, TypeInfo[] columnTypes) {
            this.dataType = dataType;
            this.columnCount = columnCount;
            this.indexes = indexes;
            if (indexes == null) {
                map = null;
            } else {
                map = new int[columnCount];
                for (int i = 0, l = indexes.length; i < l;) {
                    map[indexes[i]] = ++i;
                }
            }
            this.columnTypes = columnTypes;
        }

        @Override
        public RowFactory createRowFactory(CastDataProvider provider, CompareMode compareMode, DataHandler handler,
                Typed[] columns, IndexColumn[] indexColumns, boolean storeKeys) {
            int[] indexes = null;
            int[] sortTypes = null;
            TypeInfo[] columnTypes = null;
            int columnCount = 0;
            if (columns != null) {
                columnCount = columns.length;
                if (indexColumns == null) {
                    sortTypes = new int[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        sortTypes[i] = SortOrder.ASCENDING;
                    }
                } else {
                    int len = indexColumns.length;
                    indexes = new int[len];
                    sortTypes = new int[len];
                    for (int i = 0; i < len; i++) {
                        IndexColumn indexColumn = indexColumns[i];
                        indexes[i] = indexColumn.column.getColumnId();
                        sortTypes[i] = indexColumn.sortType;
                    }
                }
                columnTypes = new TypeInfo[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    columnTypes[i] = columns[i].getType();
                }
            }
            return createRowFactory(provider, compareMode, handler, sortTypes, indexes, columnTypes, columnCount,
                    storeKeys);
        }

        /**
         * Create a new row factory.
         *
         * @param provider the cast provider
         * @param compareMode the compare mode
         * @param handler the data handler
         * @param sortTypes the sort types
         * @param indexes the list of indexed columns
         * @param columnTypes the list of column data type information
         * @param columnCount the number of columns
         * @param storeKeys whether row keys are stored
         * @return the (possibly new) row factory
         */
        public RowFactory createRowFactory(CastDataProvider provider, CompareMode compareMode, DataHandler handler,
                int[] sortTypes, int[] indexes, TypeInfo[] columnTypes, int columnCount, boolean storeKeys) {
            RowDataType rowDataType = new RowDataType(provider, compareMode, handler, sortTypes, indexes, columnCount,
                    storeKeys);
            RowFactory rowFactory = new DefaultRowFactory(rowDataType, columnCount, indexes, columnTypes);
            rowDataType.setRowFactory(rowFactory);
            return rowFactory;
        }

        @Override
        public Row createRow(Value[] data, int memory) {
            return new DefaultRow(data, memory);
        }

        @Override
        public SearchRow createRow() {
            if (indexes == null) {
                return new DefaultRow(columnCount);
            } else if (indexes.length == 1) {
                return new SimpleRowValue(columnCount, indexes[0]);
            } else {
                return new Sparse(columnCount, indexes.length, map);
            }
        }

        @Override
        public RowDataType getRowDataType() {
            return dataType;
        }

        @Override
        public int[] getIndexes() {
            return indexes;
        }

        @Override
        public TypeInfo[] getColumnTypes() {
            return columnTypes;
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public boolean getStoreKeys() {
            return dataType.isStoreKeys();
        }
    }
}
