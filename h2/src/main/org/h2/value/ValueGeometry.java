/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.PrecisionModel;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * Implementation of the GEOMETRY data type.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class ValueGeometry extends Value implements ValueSpatial {

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
     *            com.vividsolutions.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        return get((Geometry) o);
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
     * @param s the WKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        try {
            Geometry g = new WKTReader().read(s);
            return get(g);
        } catch (ParseException ex) {
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
        try {
            GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
            Geometry g = new WKTReader(geometryFactory).read(s);
            return get(g);
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
        return (Geometry) getGeometryNoCopy().clone();
    }

    public Geometry getGeometryNoCopy() {
        if (geometry == null) {
            try {
                geometry = new WKBReader().read(bytes);
            } catch (ParseException ex) {
                throw DbException.convert(ex);
            }
        }
        return geometry;
    }

    /**
     * Test if this geometry envelope intersects with the other geometry
     * envelope.
     *
     * @param r the other geometry
     * @return true if the two overlap
     */
    public boolean intersectsBoundingBox(ValueSpatial r) {
        // the Geometry object caches the envelope
        return getGeometryNoCopy().getEnvelopeInternal().intersects(
                r.getEnvelope());
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
    public Envelope getEnvelope() {
        return getGeometryNoCopy().getEnvelopeInternal();
    }

    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        // WKT does not hold Z or SRID with JTS 1.13. As getSQL is used to
        // export database, it should contains all object attributes. Moreover
        // using bytes is faster than converting WKB to Geometry then to WKT.
        return "X'" + StringUtils.convertBytesToHex(getBytesNoCopy()) + "'::Geometry";
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        Geometry g = ((ValueGeometry) v).getGeometryNoCopy();
        return getGeometryNoCopy().compareTo(g);
    }

    @Override
    public String getString() {
        return getWKT();
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
        return getWKB();
    }

    @Override
    public byte[] getBytesNoCopy() {
        return getWKB();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setObject(parameterIndex, getGeometryNoCopy());
    }

    @Override
    public int getDisplaySize() {
        return getWKT().length();
    }

    @Override
    public int getMemory() {
        return getWKB().length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        // The JTS library only does half-way support for 3D coordinates, so
        // their equals method only checks the first two coordinates.
        return other instanceof ValueGeometry &&
                Arrays.equals(getWKB(), ((ValueGeometry) other).getWKB());
    }

    /**
     * Get the value in Well-Known-Text format.
     *
     * @return the well-known-text
     */
    public String getWKT() {
        return new WKTWriter(3).write(getGeometryNoCopy());
    }

    /**
     * Get the value in Well-Known-Binary format.
     *
     * @return the well-known-binary
     */
    public byte[] getWKB() {
        return bytes;
    }

    @Override
    public Value convertTo(int targetType) {
        if (targetType == Value.JAVA_OBJECT) {
            return this;
        }
        return super.convertTo(targetType);
    }

    /**
     * A visitor that checks if there is a Z coordinate.
     */
    static class ZVisitor implements CoordinateSequenceFilter {
        boolean foundZ;

        public boolean isFoundZ() {
            return foundZ;
        }

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
