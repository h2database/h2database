/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import org.h2.api.ErrorCode;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.geometry.EWKBUtils;
import org.h2.util.geometry.EWKTUtils;
import org.h2.util.geometry.GeometryUtils;
import org.h2.util.geometry.GeometryUtils.EnvelopeAndDimensionSystemTarget;
import org.h2.util.geometry.JTSUtils;
import org.locationtech.jts.geom.Geometry;

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
     * Dimension system. -1 if not known yet.
     */
    private int dimensionSystem;

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
     * @param dimensionSystem dimension system
     * @param envelope the envelope
     */
    private ValueGeometry(byte[] bytes, int dimensionSystem, double[] envelope) {
        this.bytes = bytes;
        this.hashCode = Arrays.hashCode(bytes);
        this.dimensionSystem = dimensionSystem;
        this.envelope = envelope;
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
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            Geometry g = (Geometry) o;
            JTSUtils.parseGeometry(g, target);
            int dimensionSystem = target.getDimensionSystem();
            return (ValueGeometry) Value.cache(new ValueGeometry(JTSUtils.geometry2ewkb(g, dimensionSystem),
                    dimensionSystem, target.getEnvelope()));
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
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            EWKTUtils.parseEWKT(s, target);
            int dimensionSystem = target.getDimensionSystem();
            return (ValueGeometry) Value.cache(new ValueGeometry(EWKTUtils.ewkt2ewkb(s, dimensionSystem),
                    dimensionSystem, target.getEnvelope()));
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
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
        return get(srid == 0 ? s : "SRID=" + srid + ';' + s);
    }

    /**
     * Get or create a geometry value for the given internal EWKB representation.
     *
     * @param bytes the WKB representation of the geometry. May not be modified.
     * @return the value
     */
    public static ValueGeometry get(byte[] bytes) {
        return (ValueGeometry) Value.cache(new ValueGeometry(bytes, -1, null));
    }

    /**
     * Get or create a geometry value for the given EWKB value.
     *
     * @param bytes the WKB representation of the geometry
     * @return the value
     */
    public static ValueGeometry getFromEWKB(byte[] bytes) {
        try {
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            EWKBUtils.parseEWKB(bytes, target);
            int dimensionSystem = target.getDimensionSystem();
            return (ValueGeometry) Value.cache(new ValueGeometry(EWKBUtils.ewkb2ewkb(bytes, dimensionSystem),
                    dimensionSystem, target.getEnvelope()));
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
        return envelope != null ? Value.cache(new ValueGeometry(GeometryUtils.envelope2wkb(envelope),
                GeometryUtils.DIMENSION_SYSTEM_XY, envelope)): ValueNull.INSTANCE;
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
                geometry = JTSUtils.ewkb2geometry(bytes, getDimensionSystem());
            } catch (RuntimeException ex) {
                throw DbException.convert(ex);
            }
        }
        return ((Geometry) geometry).copy();
    }

    private void calculateInfo() {
        if (dimensionSystem < 0) {
            EnvelopeAndDimensionSystemTarget target = new EnvelopeAndDimensionSystemTarget();
            EWKBUtils.parseEWKB(bytes, target);
            envelope = target.getEnvelope();
            dimensionSystem = target.getDimensionSystem();
        }
    }

    /**
     * Return a minimal dimension system that can be used for this geometry.
     *
     * @return dimension system
     */
    public int getDimensionSystem() {
        calculateInfo();
        return dimensionSystem;
    }

    /**
     * Return an envelope of this geometry. Do not modify the returned value.
     *
     * @return envelope of this geometry
     */
    public double[] getEnvelopeNoCopy() {
        calculateInfo();
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
    public int getType() {
        return Value.GEOMETRY;
    }

    @Override
    public String getSQL() {
        // Using bytes is faster than converting to EWKT.
        return "X'" + StringUtils.convertBytesToHex(getBytesNoCopy()) + "'::Geometry";
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        return Bits.compareNotNullUnsigned(bytes, ((ValueGeometry) v).bytes);
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
        if (DataType.GEOMETRY_CLASS != null) {
            return getGeometry();
        }
        return getEWKT();
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(bytes);
    }

    @Override
    public byte[] getBytesNoCopy() {
        return bytes;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBytes(parameterIndex, bytes);
    }

    @Override
    public int getDisplaySize() {
        return getEWKT().length();
    }

    @Override
    public int getMemory() {
        return bytes.length * 20 + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueGeometry && Arrays.equals(bytes, ((ValueGeometry) other).bytes);
    }

    /**
     * Get the value in Extended Well-Known Text format.
     *
     * @return the extended well-known text
     */
    public String getEWKT() {
        return EWKTUtils.ewkb2ewkt(bytes, getDimensionSystem());
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

}
