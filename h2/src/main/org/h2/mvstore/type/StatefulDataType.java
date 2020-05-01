/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * Interface StatefulDataType.
 *
 * @param <D> type of opaque parameter passed as an operational context to Factory.create()
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public interface StatefulDataType<D> {

    void save(WriteBuffer buff, MetaType<D> metaType);

    Factory<D> getFactory();

    interface Factory<D> {
        DataType<?> create(ByteBuffer buff, MetaType<D> metaDataType, D database);
    }
}
