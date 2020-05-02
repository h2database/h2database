/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.CastDataProvider;
import org.h2.engine.Mode;
import org.h2.mvstore.db.RowDataType;
import org.h2.store.DataHandler;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
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
     * @param mode the compatibility mode
     * @param handler the data handler
     * @param columns the list of columns
     * @param indexColumns the list of index columns
     * @return the (possibly new) row factory
     */
    public RowFactory createRowFactory(CastDataProvider provider, CompareMode compareMode, Mode mode,
            DataHandler handler, Column[] columns, IndexColumn[] indexColumns) {
        return this;
    }

    /**
     * Create new row.
     *
     * @param data the values
     * @param memory whether the row is in memory
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

    public abstract int getColumnCount();


    /**
     * Default implementation of row factory.
     */
    public static final class DefaultRowFactory extends RowFactory {
        private final RowDataType dataType;
        private final int         columnCount;
        private final int[]       indexes;
        private final int[]       map;

        public static final DefaultRowFactory INSTANCE = new DefaultRowFactory();

        DefaultRowFactory() {
            this(new RowDataType(null, CompareMode.getInstance(null, 0), Mode.getRegular(),
                                    null, null, null, 0), 0, null);
        }

        private DefaultRowFactory(RowDataType dataType, int columnCount, int[] indexes) {
            this.dataType = dataType;
            this.columnCount = columnCount;
            this.indexes = indexes;
            this.map = indexes == null ? null : new int[columnCount];
            if(indexes != null) {
                for (int i = 0; i < indexes.length; i++) {
                    map[indexes[i]] = i + 1;
                }
            }
        }

        @Override
        public RowFactory createRowFactory(CastDataProvider provider, CompareMode compareMode, Mode mode,
                DataHandler handler, Column[] columns, IndexColumn[] indexColumns) {
            int[] indexes = null;
            int[] sortTypes = null;
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
            }
            return createRowFactory(provider, compareMode, mode, handler, sortTypes, indexes, columnCount);
        }

        /**
         * Create a new row factory.
         *
         * @param provider the cast provider
         * @param compareMode the compare mode
         * @param mode the compatibility mode
         * @param handler the data handler
         * @param sortTypes the sort types
         * @param indexes the list of indexed columns
         * @param columnCount the number of columns
         * @return the (possibly new) row factory
         */
        public RowFactory createRowFactory(CastDataProvider provider, CompareMode compareMode, Mode mode,
                DataHandler handler, int[] sortTypes, int[] indexes, int columnCount) {
            RowDataType rowDataType = new RowDataType(provider, compareMode, mode, handler,
                                                    sortTypes, indexes, columnCount);
            RowFactory rowFactory = new DefaultRowFactory(rowDataType, columnCount, indexes);
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
        public int getColumnCount() {
            return columnCount;
        }
    }
}
