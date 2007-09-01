/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.index;

/**
 * @author Thomas
 */
public class IndexType {
    private boolean isPrimaryKey, isPersistent, isUnique, isHash, isScan;
    private boolean belongsToConstraint;     
    
    public static IndexType createPrimaryKey(boolean persistent, boolean hash) {
        IndexType type = new IndexType();
        type.isPrimaryKey = true;
        type.isPersistent = persistent;
        type.isHash = hash;        
        type.isUnique = true;
        return type;        
    }
    
    public static IndexType createUnique(boolean persistent, boolean hash) {
        IndexType type = new IndexType();        
        type.isUnique = true;
        type.isPersistent = persistent;
        type.isHash = hash;
        return type;
    }
    
    public static IndexType createNonUnique(boolean persistent) {
        IndexType type = new IndexType();               
        type.isPersistent = persistent;
        return type;
    }
    
    public static IndexType createScan(boolean persistent) {
        IndexType type = new IndexType();               
        type.isPersistent = persistent;
        type.isScan = true;
        return type;
    }
    
    public void setBelongsToConstraint(boolean belongsToConstraint) {
        this.belongsToConstraint = belongsToConstraint;
    }
    
    public boolean belongsToConstraint() {
        return belongsToConstraint;
    }    
    
    public boolean isHash() {
        return isHash;
    }
    public boolean isPersistent() {
        return isPersistent;
    }
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
    public boolean isUnique() {
        return isUnique;
    }
    
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

    public boolean isScan() {
        return isScan;
    }

}
