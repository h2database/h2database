/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.geometry;

import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYM;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZ;
import static org.h2.util.geometry.GeometryUtils.DIMENSION_SYSTEM_XYZM;
import static org.h2.util.geometry.GeometryUtils.GEOMETRY_COLLECTION;
import static org.h2.util.geometry.GeometryUtils.LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.MULTI_LINE_STRING;
import static org.h2.util.geometry.GeometryUtils.MULTI_POINT;
import static org.h2.util.geometry.GeometryUtils.MULTI_POLYGON;
import static org.h2.util.geometry.GeometryUtils.POINT;
import static org.h2.util.geometry.GeometryUtils.POLYGON;

import java.io.ByteArrayOutputStream;

import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.geometry.GeometryUtils.DimensionSystemTarget;
import org.h2.util.geometry.GeometryUtils.Target;

/**
 * Utilities for GEOMETRY data type.
 */
public final class EWKBUtils {

    /**
     * Converter output target that writes a EWKB.
     */
    public static final class EWKBTarget extends Target {

        private final ByteArrayOutputStream output;

        private final int dimensionSystem;

        private final byte[] buf = new byte[8];

        private int level;

        /**
         * Creates a new EWKB output target.
         *
         * @param output
         *            output stream
         * @param dimensionSystem
         *            dimension system to use
         */
        public EWKBTarget(ByteArrayOutputStream output, int dimensionSystem) {
            this.output = output;
            this.dimensionSystem = dimensionSystem;
        }

        @Override
        protected void startPoint(int srid) {
            writeHeader(POINT, srid);
        }

        @Override
        protected void startLineString(int srid, int numPoints) {
            writeHeader(LINE_STRING, srid);
            writeInt(numPoints);
        }

        @Override
        protected void startPolygon(int srid, int numInner, int numPoints) {
            writeHeader(POLYGON, srid);
            writeInt(numInner + 1);
            writeInt(numPoints);
        }

        @Override
        protected void startPolygonInner(int numInner) {
            writeInt(numInner);
        }

        @Override
        protected void startCollection(int type, int srid, int numItems) {
            writeHeader(type, srid);
            writeInt(numItems);
        }

        private void writeHeader(int type, int srid) {
            switch (dimensionSystem) {
            case DIMENSION_SYSTEM_XYZ:
                type |= EWKB_Z;
                break;
            case DIMENSION_SYSTEM_XYZM:
                type |= EWKB_Z;
                //$FALL-THROUGH$
            case DIMENSION_SYSTEM_XYM:
                type |= EWKB_M;
            }
            // Never write SRID in inner objects
            if (level > 0) {
                srid = 0;
            } else if (srid != 0) {
                type |= EWKB_SRID;
            }
            output.write(0);
            writeInt(type);
            if (srid != 0) {
                writeInt(srid);
            }
        }

        @Override
        protected Target startCollectionItem(int index, int total) {
            level++;
            return this;
        }

        @Override
        protected void endCollectionItem(Target target, int index, int total) {
            level--;
        }

        @Override
        protected void addCoordinate(double x, double y, double z, double m, int index, int total) {
            writeDouble(x);
            writeDouble(y);
            if ((dimensionSystem & DIMENSION_SYSTEM_XYZ) != 0) {
                writeDouble(z);
            }
            if ((dimensionSystem & DIMENSION_SYSTEM_XYM) != 0) {
                writeDouble(m);
            }
        }

        private void writeInt(int v) {
            Bits.writeInt(buf, 0, v);
            output.write(buf, 0, 4);
        }

        private void writeDouble(double v) {
            v = toCanonicalDouble(v);
            Bits.writeLong(buf, 0, Double.doubleToLongBits(v));
            output.write(buf, 0, 8);
        }

    }

    /**
     * Helper source object for EWKB reading.
     */
    private static final class EWKBSource {
        private final byte[] ewkb;

        private int offset;

        /**
         * Whether current byte order is big-endian.
         */
        boolean bigEndian;

        /**
         * Creates new instance of EWKB source.
         *
         * @param ewkb
         *            EWKB
         */
        EWKBSource(byte[] ewkb) {
            this.ewkb = ewkb;
        }

        /**
         * Reads one byte.
         *
         * @return next byte
         */
        byte readByte() {
            return ewkb[offset++];
        }

        /**
         * Reads a 32-bit integer using current byte order.
         *
         * @return next 32-bit integer
         */
        int readInt() {
            int result = Bits.readInt(ewkb, offset);
            offset += 4;
            return bigEndian ? result : Integer.reverseBytes(result);
        }

        /**
         * Reads a 64-bit floating point using current byte order.
         *
         * @return next 64-bit floating point
         */
        double readCoordinate() {
            long v = Bits.readLong(ewkb, offset);
            offset += 8;
            if (!bigEndian) {
                v = Long.reverseBytes(v);
            }
            return toCanonicalDouble(Double.longBitsToDouble(v));
        }

        @Override
        public String toString() {
            String s = StringUtils.convertBytesToHex(ewkb);
            int idx = offset * 2;
            return new StringBuilder(s.length() + 3).append(s, 0, idx).append("<*>").append(s, idx, s.length())
                    .toString();
        }

    }

    /**
     * Geometry type mask that indicates presence of dimension Z.
     */
    private static final int EWKB_Z = 0x8000_0000;

    /**
     * Geometry type mask that indicates presence of dimension M.
     */
    private static final int EWKB_M = 0x4000_0000;

    /**
     * Geometry type mask that indicates presence of SRID.
     */
    private static final int EWKB_SRID = 0x2000_0000;

    /**
     * Converts any supported EWKB to EWKB representation that is used by this
     * class. Reduces dimension system to minimal possible and uses EWKB flags
     * for dimension system indication. May also perform other changes.
     *
     * @param ewkb
     *            source EWKB
     * @return canonical EWKB, may be the same as the source
     */
    public static byte[] ewkb2ewkb(byte[] ewkb) {
        // Determine dimension system first
        DimensionSystemTarget dimensionTarget = new DimensionSystemTarget();
        parseEKWB(ewkb, dimensionTarget);
        // Write an EWKB
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        EWKBTarget target = new EWKBTarget(output, dimensionTarget.getDimensionSystem());
        parseEKWB(ewkb, target);
        return output.toByteArray();
    }

    /**
     * Parses a EWKB.
     *
     * @param ewkb
     *            EWKB representation
     * @param target
     *            output target
     */
    public static void parseEKWB(byte[] ewkb, Target target) {
        parseEKWB(new EWKBSource(ewkb), target, 0, 0);
    }

    /**
     * Parses a EWKB.
     *
     * @param source
     *            EWKB source
     * @param target
     *            output target
     * @param parentType
     *            type of parent geometry collection, or 0 for the root geometry
     * @param parentSrid
     *            SRID of a parent geometry collection, or any value for the
     *            root geometry (will be determined from the EWKB instead)
     */
    private static void parseEKWB(EWKBSource source, Target target, int parentType, int parentSrid) {
        try {
            // Read byte order of a next geometry
            switch (source.readByte()) {
            case 0:
                source.bigEndian = true;
                break;
            case 1:
                source.bigEndian = false;
                break;
            default:
                throw new IllegalArgumentException();
            }
            // Type contains type of a geometry and additional flags
            int type = source.readInt();
            // PostGIS extensions
            boolean useZ = (type & EWKB_Z) != 0;
            boolean useM = (type & EWKB_M) != 0;
            int srid = (type & EWKB_SRID) != 0 ? source.readInt() : 0;
            // Preserve parent SRID unconditionally
            if (parentType != 0) {
                srid = parentSrid;
            }
            // OGC 06-103r4
            type &= 0xffff;
            switch (type / 1_000) {
            case DIMENSION_SYSTEM_XYZ:
                useZ = true;
                break;
            case DIMENSION_SYSTEM_XYZM:
                useZ = true;
                //$FALL-THROUGH$
            case DIMENSION_SYSTEM_XYM:
                useM = true;
            }
            type %= 1_000;
            switch (type) {
            case POINT:
                if (parentType != 0 && parentType != MULTI_POINT && parentType != GEOMETRY_COLLECTION) {
                    throw new IllegalArgumentException();
                }
                target.startPoint(srid);
                addCoordinate(source, target, useZ, useM, 0, 1);
                break;
            case LINE_STRING: {
                if (parentType != 0 && parentType != MULTI_LINE_STRING && parentType != GEOMETRY_COLLECTION) {
                    throw new IllegalArgumentException();
                }
                int numPoints = source.readInt();
                if (numPoints < 0 || numPoints == 1) {
                    throw new IllegalArgumentException();
                }
                target.startLineString(srid, numPoints);
                for (int i = 0; i < numPoints; i++) {
                    addCoordinate(source, target, useZ, useM, i, numPoints);
                }
                break;
            }
            case POLYGON: {
                if (parentType != 0 && parentType != MULTI_POLYGON && parentType != GEOMETRY_COLLECTION) {
                    throw new IllegalArgumentException();
                }
                int numInner = source.readInt() - 1;
                if (numInner < 0) {
                    throw new IllegalArgumentException();
                }
                int size = source.readInt();
                // Size may be 0 (EMPTY) or 4+
                if (size < 0 || size >= 1 && size <= 3) {
                    throw new IllegalArgumentException();
                }
                if (size == 0 && numInner > 0) {
                    throw new IllegalArgumentException();
                }
                target.startPolygon(srid, numInner, size);
                if (size > 0) {
                    addRing(source, target, useZ, useM, size);
                    for (int i = 0; i < numInner; i++) {
                        size = source.readInt();
                        // Size may be 0 (EMPTY) or 4+
                        if (size < 0 || size >= 1 && size <= 3) {
                            throw new IllegalArgumentException();
                        }
                        target.startPolygonInner(size);
                        addRing(source, target, useZ, useM, size);
                    }
                    target.endNonEmptyPolygon();
                }
                break;
            }
            case MULTI_POINT:
            case MULTI_LINE_STRING:
            case MULTI_POLYGON:
            case GEOMETRY_COLLECTION: {
                if (parentType != 0 && parentType != GEOMETRY_COLLECTION) {
                    throw new IllegalArgumentException();
                }
                int numItems = source.readInt();
                if (numItems < 0) {
                    throw new IllegalArgumentException();
                }
                target.startCollection(type, srid, numItems);
                for (int i = 0; i < numItems; i++) {
                    Target innerTarget = target.startCollectionItem(i, numItems);
                    parseEKWB(source, innerTarget, type, srid);
                    target.endCollectionItem(innerTarget, i, numItems);
                }
                target.endCollection(type);
                break;
            }
            default:
                throw new IllegalArgumentException();
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    private static void addRing(EWKBSource source, Target target, boolean useZ, boolean useM, int size) {
        // 0 or 4+ are valid
        if (size >= 4) {
            double startX = source.readCoordinate(), startY = source.readCoordinate();
            target.addCoordinate(startX, startY, //
                    useZ ? source.readCoordinate() : Double.NaN, useM ? source.readCoordinate() : Double.NaN, //
                    0, size);
            for (int i = 1; i < size - 1; i++) {
                addCoordinate(source, target, useZ, useM, i, size);
            }
            double endX = source.readCoordinate(), endY = source.readCoordinate();
            /*
             * TODO OGC 06-103r4 determines points as equal if they have the
             * same X and Y coordinates. Should we check Z and M here too?
             */
            if (startX != endX || startY != endY) {
                throw new IllegalArgumentException();
            }
            target.addCoordinate(endX, endY, //
                    useZ ? source.readCoordinate() : Double.NaN, useM ? source.readCoordinate() : Double.NaN, //
                    size - 1, size);
        }
    }

    private static void addCoordinate(EWKBSource source, Target target, boolean useZ, boolean useM, int index,
            int total) {
        target.addCoordinate(source.readCoordinate(), source.readCoordinate(),
                useZ ? source.readCoordinate() : Double.NaN, useM ? source.readCoordinate() : Double.NaN, //
                index, total);
    }

    /**
     * Normalizes all NaNs into single type on NaN and negative zero to positive
     * zero.
     *
     * @param d
     *            double value
     * @return normalized value
     */
    static double toCanonicalDouble(double d) {
        return Double.isNaN(d) ? Double.NaN : d == 0d ? 0d : d;
    }

    private EWKBUtils() {
    }

}
