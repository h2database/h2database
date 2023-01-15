/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;

import org.h2.mvstore.WriteBuffer;

/**
 * A data type that allows to save its state.
 *
 * @param <D> type of opaque parameter passed as an operational context to Factory.create()
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public interface StatefulDataType<D> {

    /**
     * Save the state.
     *
     * @param buff the target buffer
     * @param metaType the meta type
     */
    void save(WriteBuffer buff, MetaType<D> metaType);

    Factory<D> getFactory();

    /**
     * A factory for data types.
     *
     * @param <D> the database type
     */
    interface Factory<D> {
        /**
         * Reads the data type.
         *
         * @param buff the buffer the source buffer
         * @param metaDataType the type
         * @param database the database
         * @return the data type
         */
        DataType<?> create(ByteBuffer buff, MetaType<D> metaDataType, D database);
    }
}
