/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * Implementation of the GEOMETRY data type.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueGeometry extends Value {

    /**
     * As conversion from/to WKB cost a significant amount of CPU cycles, WKB
     * are kept in ValueGeometry instance.
     *
     * We always calculate the WKB, because not all WKT values can be
     * represented in WKB, but since we persist it in WKB format, it has to be
     * valid in WKB
     */
    private final byte[] bytes;

    private final int hashCode;

    /**
     * The value. Converted from WKB only on request as conversion from/to WKB
     * cost a significant amount of CPU cycles.
     */
    private Geometry geometry;

    /**
     * Create a new geometry objects.
     *
     * @param bytes the bytes (always known)
     * @param geometry the geometry object (may be null)
     */
    private ValueGeometry(byte[] bytes, Geometry geometry) {
        this.bytes = bytes;
        this.geometry = geometry;
        this.hashCode = Arrays.hashCode(bytes);
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param o the geometry object (of type
     *            org.locationtech.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        /*
         * Do not pass untrusted source geometry object to a cache, use only its WKB
         * representation. Geometries are not fully immutable.
         */
        return get(convertToWKB((Geometry) o));
    }

    private static ValueGeometry get(Geometry g) {
        byte[] bytes = convertToWKB(g);
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes, g));
    }

    private static byte[] convertToWKB(Geometry g) {
        boolean includeSRID = g.getSRID() != 0;
        int dimensionCount = getDimensionCount(g);
        WKBWriter writer = new WKBWriter(dimensionCount, includeSRID);
        return writer.write(g);
    }

    private static int getDimensionCount(Geometry geometry) {
        ZVisitor finder = new ZVisitor();
        geometry.apply(finder);
        return finder.isFoundZ() ? 3 : 2;
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT or EWKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        try {
            int srid;
            if (s.startsWith("SRID=")) {
                int idx = s.indexOf(';', 5);
                srid = Integer.parseInt(s.substring(5, idx));
                s = s.substring(idx + 1);
            } else {
                srid = 0;
            }
            /*
             * No-arg WKTReader() constructor instantiates a new GeometryFactory and a new
             * PrecisionModel anyway, so special case for srid == 0 is not needed.
             */
            return get(new WKTReader(new GeometryFactory(new PrecisionModel(), srid)).read(s));
        } catch (ParseException | StringIndexOutOfBoundsException | NumberFormatException ex) {
            throw DbException.convert(ex);
        }
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT representation of the geometry
     * @param srid the srid of the object
     * @return the value
     */
    public static ValueGeometry get(String s, int srid) {
        // This method is not used in H2, but preserved for H2GIS
        try {
            return get(new WKTReader(new GeometryFactory(new PrecisionModel(), srid)).read(s));
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes, null));
    }

    /**
     * Get a copy of geometry object. Geometry object is mutable. The returned
     * object is therefore copied before returning.
     *
     * @return a copy of the geometry object
     */
    public Geometry getGeometry() {
        Geometry geometry = getGeometryNoCopy();
        Geometry copy = geometry.copy();
        return copy;
    }

    public Geometry getGeometryNoCopy() {
        if (geometry == null) {
            try {
                /*
                 * No-arg WKBReader() constructor instantiates a new GeometryFactory and a new
                 * PrecisionModel anyway, so special case for srid == 0 is not needed.
                 */
                geometry = new WKBReader(new GeometryFactory(new PrecisionModel(), getSRID())).read(bytes);
            } catch (ParseException ex) {
                throw DbException.convert(ex);
            }
        }
        return geometry;
    }

    /**
     * Return the SRID (Spatial Reference Identifier).
     *
     * @return spatial reference identifier
     */
    public int getSRID() {
        if (bytes.length >= 9) {
            boolean bigEndian;
            switch (bytes[0]) {
            case 0:
                bigEndian = true;
                break;
            case 1:
                bigEndian = false;
                break;
            default:
                return 0;
            }
            if ((bytes[bigEndian ? 1 : 4] & 0x20) != 0) {
                int srid = Bits.readInt(bytes, 5);
                if (!bigEndian) {
                    srid = Integer.reverseBytes(srid);
                }
                return srid;
            }
        }
        return 0;
    }

    /**
     * Test if this geometry envelope intersects with the other geometry
     * envelope.
     *
     * @param r the other geometry
     * @return true if the two overlap
     */
    public boolean intersectsBoundingBox(ValueGeometry r) {
        // the Geometry object caches the envelope
        return getGeometryNoCopy().getEnvelopeInternal().intersects(
                r.getGeometryNoCopy().getEnvelopeInternal());
    }

    /**
     * Get the union.
     *
     * @param r the other geometry
     * @return the union of this geometry envelope and another geometry envelope
     */
    public Value getEnvelopeUnion(ValueGeometry r) {
        GeometryFactory gf = new GeometryFactory();
        Envelope mergedEnvelope = new Envelope(getGeometryNoCopy().getEnvelopeInternal());
        mergedEnvelope.expandToInclude(r.getGeometryNoCopy().getEnvelopeInternal());
        return get(gf.toGeometry(mergedEnvelope));
    }

    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        // Using bytes is faster than converting EWKB to Geometry then EWKT.
        return "X'" + StringUtils.convertBytesToHex(getBytesNoCopy()) + "'::Geometry";
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        Geometry g = ((ValueGeometry) v).getGeometryNoCopy();
        return getGeometryNoCopy().compareTo(g);
    }

    @Override
    public String getString() {
        return getEWKT();
    }

    @Override
    public long getPrecision() {
        return 0;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public Object getObject() {
        return getGeometry();
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(getEWKB());
    }

    @Override
    public byte[] getBytesNoCopy() {
        return getEWKB();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setObject(parameterIndex, getGeometryNoCopy());
    }

    @Override
    public int getDisplaySize() {
        return getEWKT().length();
    }

    @Override
    public int getMemory() {
        return getEWKB().length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        // The JTS library only does half-way support for 3D coordinates, so
        // their equals method only checks the first two coordinates.
        return other instanceof ValueGeometry &&
                Arrays.equals(getEWKB(), ((ValueGeometry) other).getEWKB());
    }

    /**
     * Get the value in Extended Well-Known Text format.
     *
     * @return the extended well-known text
     */
    public String getEWKT() {
        String wkt = new WKTWriter(3).write(getGeometryNoCopy());
        int srid = getSRID();
        return srid == 0
                ? wkt
                // "SRID=-2147483648;".length() == 17
                : new StringBuilder(wkt.length() + 17).append("SRID=").append(srid).append(';').append(wkt).toString();
    }

    /**
     * Get the value in extended Well-Known Binary format.
     *
     * @return the extended well-known binary
     */
    public byte[] getEWKB() {
        return bytes;
    }

    @Override
    public Value convertTo(int targetType, int precision, Mode mode, Object column, String[] enumerators) {
        if (targetType == Value.JAVA_OBJECT) {
            return this;
        }
        return super.convertTo(targetType, precision, mode, column, null);
    }

    /**
     * A visitor that checks if there is a Z coordinate.
     */
    static class ZVisitor implements CoordinateSequenceFilter {

        private boolean foundZ;

        public boolean isFoundZ() {
            return foundZ;
        }

        /**
         * Performs an operation on a coordinate in a CoordinateSequence.
         *
         * @param coordinateSequence the object to which the filter is applied
         * @param i the index of the coordinate to apply the filter to
         */
        @Override
        public void filter(CoordinateSequence coordinateSequence, int i) {
            if (!Double.isNaN(coordinateSequence.getOrdinate(i, 2))) {
                foundZ = true;
            }
        }

        @Override
        public boolean isDone() {
            return foundZ;
        }

        @Override
        public boolean isGeometryChanged() {
            return false;
        }

    }

}
