/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import static org.h2.util.geometry.EWKBUtils.EWKB_SRID;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.geometry.EWKBUtils;
import org.h2.util.geometry.EWKTUtils;
import org.h2.util.geometry.GeometryUtils;
import org.h2.util.geometry.GeometryUtils.EnvelopeTarget;
import org.h2.util.geometry.JTSUtils;
import org.h2.util.geometry.EWKTUtils.EWKTTarget;
import org.locationtech.jts.geom.Geometry;

/**
 * Implementation of the GEOMETRY data type.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public final class ValueGeometry extends ValueBytesBase {

    private static final double[] UNKNOWN_ENVELOPE = new double[0];

    /**
     * Geometry type and dimension system in OGC geometry code format (type +
     * dimensionSystem * 1000).
     */
    private final int typeAndDimensionSystem;

    /**
     * Spatial reference system identifier.
     */
    private final int srid;

    /**
     * The envelope of the value. Calculated only on request.
     */
    private double[] envelope;

    /**
     * The value. Converted from WKB only on request as conversion from/to WKB
     * cost a significant amount of CPU cycles.
     */
    private Object geometry;

    /**
     * Create a new geometry object.
     *
     * @param bytes the EWKB bytes
     * @param envelope the envelope
     */
    private ValueGeometry(byte[] bytes, double[] envelope) {
        super(bytes);
        if (bytes.length < 9 || bytes[0] != 0) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, StringUtils.convertBytesToHex(bytes));
        }
        this.value = bytes;
        this.envelope = envelope;
        int t = Bits.readInt(bytes, 1);
        srid = (t & EWKB_SRID) != 0 ? Bits.readInt(bytes, 5) : 0;
        typeAndDimensionSystem = (t & 0xffff) % 1_000 + EWKBUtils.type2dimensionSystem(t) * 1_000;
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param o the geometry object (of type
     *            org.locationtech.jts.geom.Geometry)
     * @return the value
     */
    public static ValueGeometry getFromGeometry(Object o) {
        try {
            Geometry g = (Geometry) o;
            return (ValueGeometry) Value.cache(new ValueGeometry(JTSUtils.geometry2ewkb(g), UNKNOWN_ENVELOPE));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, String.valueOf(o));
        }
    }

    /**
     * Get or create a geometry value for the given geometry.
     *
     * @param s the WKT or EWKT representation of the geometry
     * @return the value
     */
    public static ValueGeometry get(String s) {
        try {
            return (ValueGeometry) Value.cache(new ValueGeometry(EWKTUtils.ewkt2ewkb(s), UNKNOWN_ENVELOPE));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
    }

    /**
     * Get or create a geometry value for the given internal EWKB representation.
     *
     * @param bytes the WKB representation of the geometry. May not be modified.
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes, UNKNOWN_ENVELOPE));
    }

    /**
     * Get or create a geometry value for the given EWKB value.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry getFromEWKB(byte[] bytes) {
        try {
            return (ValueGeometry) Value.cache(new ValueGeometry(EWKBUtils.ewkb2ewkb(bytes), UNKNOWN_ENVELOPE));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, StringUtils.convertBytesToHex(bytes));
        }
    }

    /**
     * Creates a geometry value for the given envelope.
     *
     * @param envelope envelope. May not be modified.
     * @return the value
     */
    public static Value fromEnvelope(double[] envelope) {
        return envelope != null
                ? Value.cache(new ValueGeometry(EWKBUtils.envelope2wkb(envelope), envelope))
                : ValueNull.INSTANCE;
    }

    /**
     * Get a copy of geometry object. Geometry object is mutable. The returned
     * object is therefore copied before returning.
     *
     * @return a copy of the geometry object
     */
    public Geometry getGeometry() {
        if (geometry == null) {
            try {
                geometry = JTSUtils.ewkb2geometry(value, getDimensionSystem());
            } catch (RuntimeException ex) {
                throw DbException.convert(ex);
            }
        }
        return ((Geometry) geometry).copy();
    }

    /**
     * Returns geometry type and dimension system in OGC geometry code format
     * (type + dimensionSystem * 1000).
     *
     * @return geometry type and dimension system
     */
    public int getTypeAndDimensionSystem() {
        return typeAndDimensionSystem;
    }

    /**
     * Returns geometry type.
     *
     * @return geometry type and dimension system
     */
    public int getGeometryType() {
        return typeAndDimensionSystem % 1_000;
    }

    /**
     * Return a minimal dimension system that can be used for this geometry.
     *
     * @return dimension system
     */
    public int getDimensionSystem() {
        return typeAndDimensionSystem / 1_000;
    }

    /**
     * Return a spatial reference system identifier.
     *
     * @return spatial reference system identifier
     */
    public int getSRID() {
        return srid;
    }

    /**
     * Return an envelope of this geometry. Do not modify the returned value.
     *
     * @return envelope of this geometry
     */
    public double[] getEnvelopeNoCopy() {
        if (envelope == UNKNOWN_ENVELOPE) {
            EnvelopeTarget target = new EnvelopeTarget();
            EWKBUtils.parseEWKB(value, target);
            envelope = target.getEnvelope();
        }
        return envelope;
    }

    /**
     * Test if this geometry envelope intersects with the other geometry
     * envelope.
     *
     * @param r the other geometry
     * @return true if the two overlap
     */
    public boolean intersectsBoundingBox(ValueGeometry r) {
        return GeometryUtils.intersects(getEnvelopeNoCopy(), r.getEnvelopeNoCopy());
    }

    /**
     * Get the union.
     *
     * @param r the other geometry
     * @return the union of this geometry envelope and another geometry envelope
     */
    public Value getEnvelopeUnion(ValueGeometry r) {
        return fromEnvelope(GeometryUtils.union(getEnvelopeNoCopy(), r.getEnvelopeNoCopy()));
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_GEOMETRY;
    }

    @Override
    public int getValueType() {
        return GEOMETRY;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("GEOMETRY ");
        if ((sqlFlags & ADD_PLAN_INFORMATION) != 0) {
            EWKBUtils.parseEWKB(value, new EWKTTarget(builder.append('\''), getDimensionSystem()));
            builder.append('\'');
        } else {
            super.getSQL(builder, DEFAULT_SQL_FLAGS);
        }
        return builder;
    }

    @Override
    public String getString() {
        return EWKTUtils.ewkb2ewkt(value, getDimensionSystem());
    }

    @Override
    public int getMemory() {
        return MathUtils.convertLongToInt(value.length * 20L + 24);
    }

}
