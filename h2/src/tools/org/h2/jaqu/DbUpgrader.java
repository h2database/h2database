/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: James Moger
 */
package org.h2.jaqu;

import org.h2.jaqu.Table.JQDatabase;

/**
 * Interface which defines a class to handle table changes based on model
 * versions.
 * <p>
 * An implementation of <i>DbUpgrader</i> <b>MUST</b> be annotated with the
 * <i>JQDatabase</i> annotation.  This annotation defines the expected database 
 * version number.
 *  
 */
public interface DbUpgrader {

    /**
     * Defines method interface to handle database upgrades. This method is only
     * called if your <i>DbUpgrader</i> implementation is annotated with
     * JQDatabase.
     * 
     * @param db
     * @param fromVersion
     * @param toVersion
     * @return Returns <b>true</b> for successful upgrade.<br>
     *         If update is successful, JaQu automatically updates its version
     *         registry.
     */
    public boolean upgradeDatabase(Db db, int fromVersion, int toVersion);

    /**
     * Defines method interface to handle table upgrades.
     * 
     * @param db
     * @param schema
     * @param table
     * @param fromVersion
     * @param toVersion
     * @return Returns <b>true</b> for successful upgrade.<br>
     *         If update is successful, JaQu automatically updates its version
     *         registry.
     */
    public boolean upgradeTable(Db db, String schema, String table, int fromVersion, int toVersion);

    /**
     * Default Db Upgrader.
     * <p>
     * Does <b>NOT</b> handle upgrade requests. Instead, this throws
     * RuntimeExceptions.
     */
    @JQDatabase(version = 0)
    public static class DefaultDbUpgrader implements DbUpgrader {

        @Override
        public boolean upgradeDatabase(Db db, int fromVersion, int toVersion) {
            throw new RuntimeException("Please provide your own DbUpgrader implementation.");
        }

        @Override
        public boolean upgradeTable(Db db, String schema, String table, int fromVersion, int toVersion) {
            throw new RuntimeException("Please provide your own DbUpgrader implementation.");
        }
    }
}
