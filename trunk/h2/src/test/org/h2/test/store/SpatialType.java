/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.nio.ByteBuffer;
import org.h2.dev.store.btree.DataType;
import org.h2.dev.store.btree.DataUtils;

/**
 * A spatial data type. This class supports up to 255 dimensions. Each dimension
 * can have a minimum and a maximum value of type float. For each dimension, the
 * maximum value is only stored when it is not the same as the minimum.
 */
public class SpatialType implements DataType {

    private final int dimensions;

    private SpatialType(int dimensions) {
        if (dimensions <= 0 || dimensions > 255) {
            throw new IllegalArgumentException("Dimensions: " + dimensions);
        }
        this.dimensions = dimensions;
    }

    public static SpatialType fromString(String s) {
        return new SpatialType(Integer.parseInt(s.substring(1)));
    }

    @Override
    public int compare(Object a, Object b) {
        long la = ((SpatialKey) a).id;
        long lb = ((SpatialKey) b).id;
        return la < lb ? -1 : la > lb ? 1 : 0;
    }

    public boolean equals(Object a, Object b) {
        long la = ((SpatialKey) a).id;
        long lb = ((SpatialKey) b).id;
        return la == lb;
    }

    @Override
    public int getMaxLength(Object obj) {
        return 1 + dimensions * 8 + DataUtils.MAX_VAR_LONG_LEN;
    }

    @Override
    public int getMemory(Object obj) {
        return 40 + dimensions * 4;
    }

    @Override
    public void write(ByteBuffer buff, Object obj) {
        SpatialKey k = (SpatialKey) obj;
        int flags = 0;
        for (int i = 0; i < dimensions; i++) {
            if (k.min[i] == k.max[i]) {
                flags |= 1 << i;
            }
        }
        DataUtils.writeVarInt(buff, flags);
        for (int i = 0; i < dimensions; i++) {
            buff.putFloat(k.min[i]);
            if ((flags & (1 << i)) == 0) {
                buff.putFloat(k.max[i]);
            }
        }
        DataUtils.writeVarLong(buff, k.id);
    }

    @Override
    public Object read(ByteBuffer buff) {
        SpatialKey k = new SpatialKey();
        int flags = DataUtils.readVarInt(buff);
        k.min = new float[dimensions];
        k.max = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            k.min[i] = buff.getFloat();
            if ((flags & (1 << i)) != 0) {
                k.max[i] = k.min[i];
            } else {
                k.max[i] = buff.getFloat();
            }
        }
        k.id = DataUtils.readVarLong(buff);
        return k;
    }

    @Override
    public String asString() {
        return "s" + dimensions;
    }

    public boolean isOverlap(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        for (int i = 0; i < dimensions; i++) {
            if (a.max[i] < b.min[i] || a.min[i] > b.max[i]) {
                return false;
            }
        }
        return true;
    }

    public SpatialKey copy(SpatialKey old) {
        SpatialKey k = new SpatialKey();
        k.min = new float[dimensions];
        k.max = new float[dimensions];
        System.arraycopy(old.min, 0, k.min, 0, dimensions);
        System.arraycopy(old.max, 0, k.max, 0, dimensions);
        return k;
    }

    public void increase(SpatialKey bounds, SpatialKey add) {
        for (int i = 0; i < dimensions; i++) {
            bounds.min[i] = Math.min(bounds.min[i], add.min[i]);
            bounds.max[i] = Math.max(bounds.max[i], add.max[i]);
        }
    }

    /**
     * Get the area increase by extending a to contain b.
     *
     * @param objA the bounding box
     * @param objB the object
     * @return the area
     */
    public float getAreaIncrease(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        float areaOld = 1, areaNew = 1;
        for (int i = 0; i < dimensions; i++) {
            float min = a.min[i];
            float max = a.max[i];
            areaOld *= max - min;
            min = Math.min(min,  b.min[i]);
            max = Math.max(max,  b.max[i]);
            areaNew *= max - min;
        }
        return areaNew - areaOld;
    }

    public float getCombinedArea(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        float area = 1;
        for (int i = 0; i < dimensions; i++) {
            float min = Math.min(a.min[i],  b.min[i]);
            float max = Math.max(a.max[i],  b.max[i]);
            area *= max - min;
        }
        return area;
    }

    /**
     * Check whether a contains b.
     *
     * @param objA the bounding box
     * @param objB the object
     * @return the area
     */
    public boolean contains(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        for (int i = 0; i < dimensions; i++) {
            if (a.min[i] > b.min[i] || a.max[i] < b.max[i]) {
                return false;
            }
        }
        return true;
    }

}
