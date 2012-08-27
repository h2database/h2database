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
            if (k.min(i) == k.max(i)) {
                flags |= 1 << i;
            }
        }
        DataUtils.writeVarInt(buff, flags);
        for (int i = 0; i < dimensions; i++) {
            buff.putFloat(k.min(i));
            if ((flags & (1 << i)) == 0) {
                buff.putFloat(k.max(i));
            }
        }
        DataUtils.writeVarLong(buff, k.id);
    }

    @Override
    public Object read(ByteBuffer buff) {
        int flags = DataUtils.readVarInt(buff);
        float[] minMax = new float[dimensions * 2];
        for (int i = 0; i < dimensions; i++) {
            float min = buff.getFloat();
            float max;
            if ((flags & (1 << i)) != 0) {
                max = min;
            } else {
                max = buff.getFloat();
            }
            minMax[i + i] = min;
            minMax[i + i + 1] = max;
        }
        long id = DataUtils.readVarLong(buff);
        return new SpatialKey(id, minMax);
    }

    @Override
    public String asString() {
        return "s" + dimensions;
    }

    public boolean isOverlap(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        for (int i = 0; i < dimensions; i++) {
            if (a.max(i) < b.min(i) || a.min(i) > b.max(i)) {
                return false;
            }
        }
        return true;
    }

    public void increaseBounds(Object bounds, Object add) {
        SpatialKey b = (SpatialKey) bounds;
        SpatialKey a = (SpatialKey) add;
        for (int i = 0; i < dimensions; i++) {
            b.setMin(i, Math.min(b.min(i), a.min(i)));
            b.setMax(i, Math.max(b.max(i), a.max(i)));
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
            float min = a.min(i);
            float max = a.max(i);
            areaOld *= max - min;
            min = Math.min(min,  b.min(i));
            max = Math.max(max,  b.max(i));
            areaNew *= max - min;
        }
        return areaNew - areaOld;
    }

    public float getCombinedArea(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        float area = 1;
        for (int i = 0; i < dimensions; i++) {
            float min = Math.min(a.min(i),  b.min(i));
            float max = Math.max(a.max(i),  b.max(i));
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
            if (a.min(i) > b.min(i) || a.max(i) < b.max(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether a given object is completely inside and does not touch the
     * given bound.
     *
     * @param objA the object to check
     * @param objB the bounds
     * @return true if a is completely inside b
     */
    public boolean isInside(Object objA, Object objB) {
        SpatialKey a = (SpatialKey) objA;
        SpatialKey b = (SpatialKey) objB;
        for (int i = 0; i < dimensions; i++) {
            if (a.min(i) <= b.min(i) || a.max(i) >= b.max(i)) {
                return false;
            }
        }
        return true;
    }

    public Object createBoundingBox(Object objA) {
        float[] minMax = new float[dimensions * 2];
        SpatialKey a = (SpatialKey) objA;
        for (int i = 0; i < dimensions; i++) {
            minMax[i + i] = a.min(i);
            minMax[i + i + 1] = a.max(i);
        }
        return new SpatialKey(0, minMax);
    }

}
