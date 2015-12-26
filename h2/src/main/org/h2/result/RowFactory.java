package org.h2.result;

import org.h2.value.Value;

/**
 * Creates rows.
 *
 * @author Sergi Vladykin
 */
public abstract class RowFactory {
    /**
     * Default implementation of row factory.
     */
    public static final RowFactory DEFAULT = new DefaultRowFactory();

    /**
     * Create new row.
     *
     * @param data row values
     * @param memory memory
     * @return created row
     */
    public abstract Row createRow(Value[] data, int memory);

    /**
     * Default implementation of row factory.
     */
    static final class DefaultRowFactory extends RowFactory {
        @Override
        public Row createRow(Value[] data, int memory) {
            return new RowImpl(data, memory);
        }
    }
}
