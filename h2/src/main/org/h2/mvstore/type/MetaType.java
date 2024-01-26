/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Constants;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

/**
 * Class DBMetaType is a type for values in the type registry map.
 *
 * @param <D> type of opaque parameter passed as an operational context to Factory.create()
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class MetaType<D> extends BasicDataType<DataType<?>> {

    private final D database;
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final Map<String, Object> cache = new HashMap<>();

    public MetaType(D database, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.database = database;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public int compare(DataType<?> a, DataType<?> b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMemory(DataType<?> obj) {
        return Constants.MEMORY_OBJECT;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(WriteBuffer buff, DataType<?> obj) {
        Class<?> clazz = obj.getClass();
        StatefulDataType<D> statefulDataType = null;
        if (obj instanceof StatefulDataType) {
            statefulDataType = (StatefulDataType<D>) obj;
            StatefulDataType.Factory<D> factory = statefulDataType.getFactory();
            if (factory != null) {
                clazz = factory.getClass();
            }
        }
        String className = clazz.getName();
        int len = className.length();
        buff.putVarInt(len)
            .putStringData(className, len);
        if (statefulDataType != null) {
            statefulDataType.save(buff, this);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataType<?> read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        String className = DataUtils.readString(buff, len);
        try {
            Object o = cache.get(className);
            if (o != null) {
                if (o instanceof StatefulDataType.Factory) {
                    return ((StatefulDataType.Factory<D>) o).create(buff, this, database);
                }
                return (DataType<?>) o;
            }
            Class<?> clazz = Class.forName(className);
            boolean singleton = false;
            Object obj;
            try {
                obj = clazz.getDeclaredField("INSTANCE").get(null);
                singleton = true;
            } catch (ReflectiveOperationException | NullPointerException e) {
                obj = clazz.getDeclaredConstructor().newInstance();
            }
            if (obj instanceof StatefulDataType.Factory) {
                StatefulDataType.Factory<D> factory = (StatefulDataType.Factory<D>) obj;
                cache.put(className, factory);
                return factory.create(buff, this, database);
            }
            if (singleton) {
                cache.put(className, obj);
            }
            return (DataType<?>) obj;
        } catch (ReflectiveOperationException | SecurityException | IllegalArgumentException e) {
            if (exceptionHandler != null) {
                exceptionHandler.uncaughtException(Thread.currentThread(), e);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataType<?>[] createStorage(int size) {
        return new DataType[size];
    }
}
