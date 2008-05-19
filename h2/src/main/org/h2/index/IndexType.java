/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

/**
 * Represents information about the properties of an index
 */
public class IndexType {
    private boolean isPrimaryKey, isPersistent, isUnique, isHash, isScan;
    private boolean belongsToConstraint;

    /**
     * Create a primary key index.
     * 
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createPrimaryKey(boolean persistent, boolean hash) {
        IndexType type = new IndexType();
        type.isPrimaryKey = true;
        type.isPersistent = persistent;
        type.isHash = hash;
        type.isUnique = true;
        return type;
    }

    /**
     * Create a unique index.
     * 
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createUnique(boolean persistent, boolean hash) {
        IndexType type = new IndexType();
        type.isUnique = true;
        type.isPersistent = persistent;
        type.isHash = hash;
        return type;
    }

    /**
     * Create a non-unique index.
     * 
     * @param persistent if the index is persistent
     * @return the index type
     */
    public static IndexType createNonUnique(boolean persistent) {
        IndexType type = new IndexType();
        type.isPersistent = persistent;
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
        type.isPersistent = persistent;
        type.isScan = true;
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
    public boolean belongsToConstraint() {
        return belongsToConstraint;
    }

    /**
     * Is this a hash index?
     * 
     * @return true if it is a hash index
     */
    public boolean isHash() {
        return isHash;
    }
    
    /**
     * Is this index persistent?
     * 
     * @return true if it is persistent
     */
    public boolean isPersistent() {
        return isPersistent;
    }
    
    /**
     * Does this index belong to a primary key constraint?
     * 
     * @return true if it references a primary key constraint
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
    
    /**
     * Is this a unique index?
     * 
     * @return true if it is
     */
    public boolean isUnique() {
        return isUnique;
    }
    
    /**
     * Get the SQL snipped to create such an index.
     * 
     * @return the SQL snipped
     */
    public String getSQL() {
        StringBuffer buff = new StringBuffer();
        if (isPrimaryKey) {
            buff.append("PRIMARY KEY");
            if (isHash) {
                buff.append(" HASH");
            }
        } else {
            if (isUnique) {
                buff.append("UNIQUE ");
            }
            if (isHash) {
                buff.append("HASH ");
            }
            buff.append("INDEX");
        }
        return buff.toString();
    }

    /**
     * Is this a table scan pseudo-index?
     * 
     * @return true if it is
     */
    public boolean isScan() {
        return isScan;
    }

}
