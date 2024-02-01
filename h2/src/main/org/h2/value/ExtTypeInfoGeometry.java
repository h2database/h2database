/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Objects;

import org.h2.util.geometry.EWKTUtils;

/**
 * Extended parameters of the GEOMETRY data type.
 */
public final class ExtTypeInfoGeometry extends ExtTypeInfo {

    private final int type;

    private final Integer srid;

    static StringBuilder toSQL(StringBuilder builder, int type, Integer srid) {
        if (type == 0 && srid == null) {
            return builder;
        }
        builder.append('(');
        if (type == 0) {
            builder.append("GEOMETRY");
        } else {
            EWKTUtils.formatGeometryTypeAndDimensionSystem(builder, type);
        }
        if (srid != null) {
            builder.append(", ").append((int) srid);
        }
        return builder.append(')');
    }

    /**
     * Creates new instance of extended parameters of the GEOMETRY data type.
     *
     * @param type
     *            the type and dimension system of geometries, or 0 if not
     *            constrained
     * @param srid
     *            the SRID of geometries, or {@code null} if not constrained
     */
    public ExtTypeInfoGeometry(int type, Integer srid) {
        this.type = type;
        this.srid = srid;
    }

    @Override
    public int hashCode() {
        return 31 * ((srid == null) ? 0 : srid.hashCode()) + type;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != ExtTypeInfoGeometry.class) {
            return false;
        }
        ExtTypeInfoGeometry other = (ExtTypeInfoGeometry) obj;
        return type == other.type && Objects.equals(srid, other.srid);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return toSQL(builder, type, srid);
    }

    /**
     * Returns the type and dimension system of geometries.
     *
     * @return the type and dimension system of geometries, or 0 if not
     *         constrained
     */
    public int getType() {
        return type;
    }

    /**
     * Returns the SRID of geometries.
     *
     * @return the SRID of geometries, or {@code null} if not constrained
     */
    public Integer getSrid() {
        return srid;
    }

}
