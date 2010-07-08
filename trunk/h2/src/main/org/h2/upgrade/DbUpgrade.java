/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.upgrade;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import org.h2.util.Utils;

/**
 * This class starts the conversion from older database versions to the current
 * version if the respective classes are found.
 */
public class DbUpgrade {

    private static boolean nonPageStoreToCurrentEnabled;

    private static Map<String, DbUpgradeNonPageStoreToCurrent> runningConversions;

    static {
        // static initialize block
        nonPageStoreToCurrentEnabled = Utils.isClassPresent("org.h2.upgrade.v1_1.Driver");
        runningConversions = Collections.synchronizedMap(new Hashtable<String, DbUpgradeNonPageStoreToCurrent>(1));
    }

    /**
     * Starts the conversion if the respective classes are found. Is automatically
     * called on connect.
     *
     * @param url The connection string
     * @param info The connection Properties
     * @throws SQLException
     */
    public static synchronized void upgrade(String url, Properties info) throws SQLException {
        if (nonPageStoreToCurrentEnabled) {
            upgradeFromNonPageStore(url, info);
        }
    }

    private static void upgradeFromNonPageStore(String url, Properties info) throws SQLException {
        if (runningConversions.containsKey(url)) {
            // do not migrate, because we are currently migrating, and this is
            // the connection where "runscript from" will be executed
            return;
        }
        try {
            DbUpgradeNonPageStoreToCurrent instance = new DbUpgradeNonPageStoreToCurrent(url, info);
            runningConversions.put(url, instance);
            instance.upgrade();
        } finally {
            runningConversions.remove(url);
        }
    }

}
