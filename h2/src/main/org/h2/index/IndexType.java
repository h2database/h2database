/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.Objects;

import org.h2.engine.NullsDistinct;

/**
 * Represents information about the properties of an index
 */
public class IndexType {

    private boolean primaryKey, persistent, hash, scan, spatial;
    private boolean belongsToConstraint;
    private NullsDistinct nullsDistinct;

    /**
     * Create a primary key index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createPrimaryKey(boolean persistent, boolean hash) {
        IndexType type = new IndexType();
        type.primaryKey = true;
        type.persistent = persistent;
        type.hash = hash;
        return type;
    }

    /**
     * Create a unique index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @param uniqueColumnCount count of unique columns (not stored)
     * @param nullsDistinct are nulls distinct
     * @return the index type
     */
    public static IndexType createUnique(boolean persistent, boolean hash, int uniqueColumnCount,
            NullsDistinct nullsDistinct) {
        IndexType type = new IndexType();
        type.persistent = persistent;
        type.hash = hash;
        type.nullsDistinct = uniqueColumnCount == 1 && nullsDistinct == NullsDistinct.ALL_DISTINCT
                ? NullsDistinct.DISTINCT
                : Objects.requireNonNull(nullsDistinct);
        return type;
    }

    /**
     * Create a non-unique index.
     *
     * @param persistent if the index is persistent
     * @return the index type
     */
    public static IndexType createNonUnique(boolean persistent) {
        return createNonUnique(persistent, false, false);
    }

    /**
     * Create a non-unique index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @param spatial if a spatial index should be used
     * @return the index type
     */
    public static IndexType createNonUnique(boolean persistent, boolean hash,
            boolean spatial) {
        IndexType type = new IndexType();
        type.persistent = persistent;
        type.hash = hash;
        type.spatial = spatial;
        return type;
    }

    /**
     * Create a scan pseudo-index.
     *
     * @param persistent if the index is persistent
     * @return the index type
     */
    public static IndexType createScan(boolean persistent) {
        IndexType type = new IndexType();
        type.persistent = persistent;
        type.scan = true;
        return type;
    }

    /**
     * Sets if this index belongs to a constraint.
     *
     * @param belongsToConstraint if the index belongs to a constraint
     */
    public void setBelongsToConstraint(boolean belongsToConstraint) {
        this.belongsToConstraint = belongsToConstraint;
    }

    /**
     * If the index is created because of a constraint. Such indexes are to be
     * dropped once the constraint is dropped.
     *
     * @return if the index belongs to a constraint
     */
    public boolean getBelongsToConstraint() {
        return belongsToConstraint;
    }

    /**
     * Is this a hash index?
     *
     * @return true if it is a hash index
     */
    public boolean isHash() {
        return hash;
    }

    /**
     * Is this a spatial index?
     *
     * @return true if it is a spatial index
     */
    public boolean isSpatial() {
        return spatial;
    }

    /**
     * Is this index persistent?
     *
     * @return true if it is persistent
     */
    public boolean isPersistent() {
        return persistent;
    }

    /**
     * Does this index belong to a primary key constraint?
     *
     * @return true if it references a primary key constraint
     */
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * Is this a unique index?
     *
     * @return true if it is
     */
    public boolean isUnique() {
        return primaryKey || nullsDistinct != null;
    }

    /**
     * Get the SQL snippet to create such an index.
     *
     * @param addNullsDistinct {@code true} to add nulls distinct clause
     * @return the SQL snippet
     */
    public String getSQL(boolean addNullsDistinct) {
        StringBuilder builder = new StringBuilder();
        if (primaryKey) {
            builder.append("PRIMARY KEY");
            if (hash) {
                builder.append(" HASH");
            }
        } else {
            if (nullsDistinct != null) {
                builder.append("UNIQUE ");
                if (addNullsDistinct) {
                    nullsDistinct.getSQL(builder, 0).append(' ');
                }
            }
            if (hash) {
                builder.append("HASH ");
            }
            if (spatial) {
                builder.append("SPATIAL ");
            }
            builder.append("INDEX");
        }
        return builder.toString();
    }

    /**
     * Is this a table scan pseudo-index?
     *
     * @return true if it is
     */
    public boolean isScan() {
        return scan;
    }

    /**
     * Returns nulls distinct treatment for unique indexes (excluding primary key indexes).
     * For primary key and other types of indexes returns {@code null}.
     *
     * @return are nulls distinct, or {@code null} for non-unique and primary key indexes
     */
    public NullsDistinct getNullsDistinct() {
        return nullsDistinct;
    }

    /**
     * Returns nulls distinct treatment for unique indexes,
     * {@link NullsDistinct#NOT_DISTINCT} for primary key indexes,
     * and {@code null} for other types of indexes.
     *
     * @return are nulls distinct, or {@code null} for non-unique indexes
     */
    public NullsDistinct getEffectiveNullsDistinct() {
        return nullsDistinct != null ? nullsDistinct : primaryKey ? NullsDistinct.NOT_DISTINCT : null;
    }

}
