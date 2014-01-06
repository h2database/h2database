/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
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
public class ValueGeometry extends Value {

    /**
     * The value. Converted from WKB only on request as conversion from/to WKB
     * cost a significant amount of cpu cycles.
     */
    private Geometry geometry;

    /**
     * As conversion from/to WKB cost a significant amount of cpu cycles, WKB
     * are kept in ValueGeometry instance
     */
    private byte[] bytes;

    private int hashCode;

    private ValueGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

    private ValueGeometry(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param o the geometry object (of type com.vividsolutions.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        return get((Geometry) o);
    }

    private static ValueGeometry get(Geometry g) {
        // not all WKT values can be represented in WKB, but since we persist it
        // in WKB format, it has to be valid in WKB
        toWKB(g);
        return (ValueGeometry) Value.cache(new ValueGeometry(g));
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        Geometry g = fromWKT(s);
        // not all WKT values can be represented in WKB, but since we persist it
        // in WKB format, it has to be valid in WKB
        toWKB(g);
        return (ValueGeometry) Value.cache(new ValueGeometry(g));
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes));
    }

    public Geometry getGeometry() {
        if (geometry == null && bytes != null) {
            geometry = fromWKB(bytes);
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
    public boolean intersectsBoundingBox(ValueGeometry r) {
        // the Geometry object caches the envelope
        return getGeometry().getEnvelopeInternal().intersects(r.getGeometry().getEnvelopeInternal());
    }

    /**
     * Get the union.
     *
     * @param r the other geometry
     * @return the union of this geometry envelope and another geometry envelope
     */
    public Value getEnvelopeUnion(ValueGeometry r) {
        GeometryFactory gf = new GeometryFactory();
        Envelope mergedEnvelope = new Envelope(getGeometry().getEnvelopeInternal());
        mergedEnvelope.expandToInclude(r.getGeometry().getEnvelopeInternal());
        return get(gf.toGeometry(mergedEnvelope));
    }

    /**
     * Get the intersection.
     *
     * @param r the other geometry
     * @return the intersection of this geometry envelope and another
     */
    public ValueGeometry getEnvelopeIntersection(ValueGeometry r) {
        Envelope e1 = getGeometry().getEnvelopeInternal();
        Envelope e2 = r.getGeometry().getEnvelopeInternal();
        Envelope e3 = e1.intersection(e2);
        // try to re-use the object
        if (e3 == e1) {
            return this;
        } else if (e3 == e2) {
            return r;
        }
        GeometryFactory gf = new GeometryFactory();
        return get(gf.toGeometry(e3));
    }

    @Override
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        return StringUtils.quoteStringSQL(toWKT()) + "'::Geometry";
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        Geometry g = ((ValueGeometry) v).getGeometry();
        return getGeometry().compareTo(g);
    }

    @Override
    public String getString() {
        return toWKT();
    }

    @Override
    public long getPrecision() {
        return 0;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(toWKB());
        }
        return hashCode;
    }

    @Override
    public Object getObject() {
        return getGeometry();
    }

    @Override
    public byte[] getBytes() {
        return toWKB();
    }

    @Override
    public byte[] getBytesNoCopy() {
        return toWKB();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setObject(parameterIndex, getGeometry());
    }

    @Override
    public int getDisplaySize() {
        return toWKT().length();
    }

    @Override
    public int getMemory() {
        return toWKB().length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        // The JTS library only does half-way support for 3D coordinates, so
        // their equals method only checks the first two coordinates.
        return other instanceof ValueGeometry && Arrays.equals(toWKB(), ((ValueGeometry) other).toWKB());
    }

    /**
     * Convert the value to the Well-Known-Text format.
     *
     * @return the well-known-text
     */
    public String toWKT() {
        return new WKTWriter().write(getGeometry());
    }

    /**
     * Convert to Well-Known-Binary format.
     *
     * @return the well-known-binary
     */
    public byte[] toWKB() {
        if (bytes != null) {
            return bytes;
        }
        return toWKB(getGeometry());
    }

    private static byte[] toWKB(Geometry geometry) {
        int dimensionCount = getDimensionCount(geometry);
        boolean includeSRID = geometry.getSRID() != 0;
        WKBWriter writer = new WKBWriter(dimensionCount, includeSRID);
        return writer.write(geometry);
    }

    private static int getDimensionCount(Geometry geometry) {
        ZVisitor finder = new ZVisitor();
        geometry.apply(finder);
        return finder.isFoundZ() ? 3 : 2;
    }

    /**
     * Convert a Well-Known-Text to a Geometry object.
     *
     * @param s the well-known-text
     * @return the Geometry object
     */
    private static Geometry fromWKT(String s) {
        try {
            return new WKTReader().read(s);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
    }

    /**
     * Convert a Well-Known-Binary to a Geometry object.
     *
     * @param bytes the well-known-binary
     * @return the Geometry object
     */
    private static Geometry fromWKB(byte[] bytes) {
        try {
            return new WKBReader().read(bytes);
        } catch (ParseException ex) {
            throw DbException.convert(ex);
        }
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
