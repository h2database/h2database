/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.engine.Database;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import java.nio.ByteBuffer;

/**
 * Interface StatefulDataType.
 * <UL>
 * <LI> 8/11/17 5:06 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public interface StatefulDataType {

    void save(WriteBuffer buff, DataType<DataType<?>> metaDataType, Database database);

    void load(ByteBuffer buff, DataType<DataType<?>> metaDataType, Database database);

    Factory getFactory();

    boolean equals(Object obj);

    int hashCode();

    interface Factory
    {
        DataType<?> create(ByteBuffer buff, DataType<DataType<?>> metaDataType, Database database);
    }
}
