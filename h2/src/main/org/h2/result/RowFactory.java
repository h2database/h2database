package org.h2.result;

import org.h2.value.Value;

/**
 * Creates rows.
 *
 * @author Sergi Vladykin
 */
public interface RowFactory {
    /**
     * Default implementation of row factory.
     */
    RowFactory DEFAULT = new RowFactory() {
        @Override
        public Row createRow(Value[] data, int memory) {
            return new RowImpl(data, memory);
        }
    };

    /**
     * Create new row.
     *
     * @param data row values
     * @param memory memory
     * @return created row
     */
    Row createRow(Value[] data, int memory);
}
