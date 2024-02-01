/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.rtree;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

/**
 * A spatial data type. This class supports up to 31 dimensions. Each dimension
 * can have a minimum and a maximum value of type float. For each dimension, the
 * maximum value is only stored when it is not the same as the minimum.
 */
public class SpatialDataType extends BasicDataType<Spatial> {

    private final int dimensions;

    public SpatialDataType(int dimensions) {
        // Because of how we are storing the
        // min-max-flag in the read/write method
        // the number of dimensions must be < 32.
        DataUtils.checkArgument(
                dimensions >= 1 && dimensions < 32,
                "Dimensions must be between 1 and 31, is {0}", dimensions);
        this.dimensions = dimensions;
    }

    /**
     * Creates spatial object with specified parameters.
     *
     * @param id the ID
     * @param minMax min x, max x, min y, max y, and so on
     * @return the spatial object
     */
    protected Spatial create(long id, float... minMax) {
        return new DefaultSpatial(id, minMax);
    }

    @Override
    public Spatial[] createStorage(int size) {
        return new Spatial[size];
    }

    @Override
    public int compare(Spatial a, Spatial b) {
        if (a == b) {
            return 0;
        } else if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        long la = a.getId();
        long lb = b.getId();
        return Long.compare(la, lb);
    }

    /**
     * Check whether two spatial values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @return true if they are equal
     */
    public boolean equals(Spatial a, Spatial b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return a.getId() == b.getId();
    }

    @Override
    public int getMemory(Spatial obj) {
        return 40 + dimensions * 4;
    }

    @Override
    public void write(WriteBuffer buff, Spatial k) {
        if (k.isNull()) {
            buff.putVarInt(-1);
            buff.putVarLong(k.getId());
            return;
        }
        int flags = 0;
        for (int i = 0; i < dimensions; i++) {
            if (k.min(i) == k.max(i)) {
                flags |= 1 << i;
            }
        }
        buff.putVarInt(flags);
        for (int i = 0; i < dimensions; i++) {
            buff.putFloat(k.min(i));
            if ((flags & (1 << i)) == 0) {
                buff.putFloat(k.max(i));
            }
        }
        buff.putVarLong(k.getId());
    }

    @Override
    public Spatial read(ByteBuffer buff) {
        int flags = DataUtils.readVarInt(buff);
        if (flags == -1) {
            long id = DataUtils.readVarLong(buff);
            return create(id);
        }
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
        return create(id, minMax);
    }

    /**
     * Check whether the two objects overlap.
     *
     * @param a the first object
     * @param b the second object
     * @return true if they overlap
     */
    public boolean isOverlap(Spatial a, Spatial b) {
        if (a.isNull() || b.isNull()) {
            return false;
        }
        for (int i = 0; i < dimensions; i++) {
            if (a.max(i) < b.min(i) || a.min(i) > b.max(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Increase the bounds in the given spatial object.
     *
     * @param bounds the bounds (may be modified)
     * @param add the value
     */
    public void increaseBounds(Spatial bounds, Spatial add) {
        if (add.isNull() || bounds.isNull()) {
            return;
        }
        for (int i = 0; i < dimensions; i++) {
            float v = add.min(i);
            if (v < bounds.min(i)) {
                bounds.setMin(i, v);
            }
            v = add.max(i);
            if (v > bounds.max(i)) {
                bounds.setMax(i, v);
            }
        }
    }

    /**
     * Get the area increase by extending a to contain b.
     *
     * @param bounds the bounding box
     * @param add the object
     * @return the area
     */
    public float getAreaIncrease(Spatial bounds, Spatial add) {
        if (bounds.isNull() || add.isNull()) {
            return 0;
        }
        float min = bounds.min(0);
        float max = bounds.max(0);
        float areaOld = max - min;
        min = Math.min(min,  add.min(0));
        max = Math.max(max,  add.max(0));
        float areaNew = max - min;
        for (int i = 1; i < dimensions; i++) {
            min = bounds.min(i);
            max = bounds.max(i);
            areaOld *= max - min;
            min = Math.min(min,  add.min(i));
            max = Math.max(max,  add.max(i));
            areaNew *= max - min;
        }
        return areaNew - areaOld;
    }

    /**
     * Get the combined area of both objects.
     *
     * @param a the first object
     * @param b the second object
     * @return the area
     */
    float getCombinedArea(Spatial a, Spatial b) {
        if (a.isNull()) {
            return getArea(b);
        } else if (b.isNull()) {
            return getArea(a);
        }
        float area = 1;
        for (int i = 0; i < dimensions; i++) {
            float min = Math.min(a.min(i),  b.min(i));
            float max = Math.max(a.max(i),  b.max(i));
            area *= max - min;
        }
        return area;
    }

    private float getArea(Spatial a) {
        if (a.isNull()) {
            return 0;
        }
        float area = 1;
        for (int i = 0; i < dimensions; i++) {
            area *= a.max(i) - a.min(i);
        }
        return area;
    }

    /**
     * Check whether bounds contains object.
     *
     * @param bounds the bounding box
     * @param object the object
     * @return the area
     */
    public boolean contains(Spatial bounds, Spatial object) {
        if (bounds.isNull() || object.isNull()) {
            return false;
        }
        for (int i = 0; i < dimensions; i++) {
            if (bounds.min(i) > object.min(i) || bounds.max(i) < object.max(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether object is completely inside bounds and does not touch them.
     *
     * @param object the object to check
     * @param bounds the bounds
     * @return true if a is completely inside b
     */
    public boolean isInside(Spatial object, Spatial bounds) {
        if (object.isNull() || bounds.isNull()) {
            return false;
        }
        for (int i = 0; i < dimensions; i++) {
            if (object.min(i) <= bounds.min(i) || object.max(i) >= bounds.max(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create a bounding box starting with the given object.
     *
     * @param object the object
     * @return the bounding box
     */
    Spatial createBoundingBox(Spatial object) {
        if (object.isNull()) {
            return object;
        }
        return object.clone(0);
    }

    /**
     * Get the most extreme pair (elements that are as far apart as possible).
     * This method is used to split a page (linear split). If no extreme objects
     * could be found, this method returns null.
     *
     * @param list the objects
     * @return the indexes of the extremes
     */
    public int[] getExtremes(ArrayList<Spatial> list) {
        list = getNotNull(list);
        if (list.isEmpty()) {
            return null;
        }
        Spatial bounds = createBoundingBox(list.get(0));
        Spatial boundsInner = createBoundingBox(bounds);
        for (int i = 0; i < dimensions; i++) {
            float t = boundsInner.min(i);
            boundsInner.setMin(i, boundsInner.max(i));
            boundsInner.setMax(i, t);
        }
        for (Spatial o : list) {
            increaseBounds(bounds, o);
            increaseMaxInnerBounds(boundsInner, o);
        }
        double best = 0;
        int bestDim = 0;
        for (int i = 0; i < dimensions; i++) {
            float inner = boundsInner.max(i) - boundsInner.min(i);
            if (inner < 0) {
                continue;
            }
            float outer = bounds.max(i) - bounds.min(i);
            float d = inner / outer;
            if (d > best) {
                best = d;
                bestDim = i;
            }
        }
        if (best <= 0) {
            return null;
        }
        float min = boundsInner.min(bestDim);
        float max = boundsInner.max(bestDim);
        int firstIndex = -1, lastIndex = -1;
        for (int i = 0; i < list.size() &&
                (firstIndex < 0 || lastIndex < 0); i++) {
            Spatial o = list.get(i);
            if (firstIndex < 0 && o.max(bestDim) == min) {
                firstIndex = i;
            } else if (lastIndex < 0 && o.min(bestDim) == max) {
                lastIndex = i;
            }
        }
        return new int[] { firstIndex, lastIndex };
    }

    private static ArrayList<Spatial> getNotNull(ArrayList<Spatial> list) {
        boolean foundNull = false;
        for (Spatial o : list) {
            if (o.isNull()) {
                foundNull = true;
                break;
            }
        }
        if (!foundNull) {
            return list;
        }
        ArrayList<Spatial> result = new ArrayList<>();
        for (Spatial o : list) {
            if (!o.isNull()) {
                result.add(o);
            }
        }
        return result;
    }

    private void increaseMaxInnerBounds(Spatial bounds, Spatial add) {
        for (int i = 0; i < dimensions; i++) {
            bounds.setMin(i, Math.min(bounds.min(i), add.max(i)));
            bounds.setMax(i, Math.max(bounds.max(i), add.min(i)));
        }
    }

}
