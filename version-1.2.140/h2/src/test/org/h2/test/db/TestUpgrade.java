/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.util.Utils;

/**
 * Automatic upgrade test cases.
 */
public class TestUpgrade extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        if (!Utils.isClassPresent("org.h2.upgrade.v1_1.Driver")) {
            return;
        }
        testNoDb();
        testNoUpgradeOldAndNew();
        testIfExists();
    }

    private void testNoDb() throws SQLException {
        deleteDb("upgrade");
        Connection conn = getConnection("upgrade");
        conn.close();
        assertTrue(new File("data/test/upgrade.h2.db").exists());
        deleteDb("upgrade");

        conn = getConnection("upgrade;NO_UPGRADE=TRUE");
        conn.close();
        assertTrue(new File("data/test/upgrade.h2.db").exists());
        deleteDb("upgrade");
    }

    private void testNoUpgradeOldAndNew() throws Exception {
        deleteDb("upgrade");
        deleteDb("upgradeOld");
        String additionalParameters = ";AUTO_SERVER=TRUE;OPEN_NEW=TRUE";

        // Create old db
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.load");
        Connection connOld = DriverManager.getConnection("jdbc:h2v1_1:data/test/upgradeOld;PAGE_STORE=FALSE" + additionalParameters);
        // Test auto server, too
        Connection connOld2 = DriverManager.getConnection("jdbc:h2v1_1:data/test/upgradeOld;PAGE_STORE=FALSE" + additionalParameters);
        Statement statOld = connOld.createStatement();
        statOld.execute("create table testOld(id int)");
        connOld.close();
        connOld2.close();
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.unload");
        assertTrue(new File("data/test/upgradeOld.data.db").exists());

        // Connect to old DB without upgrade
        Connection connOldViaNew = DriverManager.getConnection("jdbc:h2:data/test/upgradeOld;NO_UPGRADE=TRUE" + additionalParameters);
        // Test auto server, too
        Connection connOldViaNew2 = DriverManager.getConnection("jdbc:h2:data/test/upgradeOld;NO_UPGRADE=TRUE" + additionalParameters);
        Statement statOldViaNew = connOldViaNew.createStatement();
        statOldViaNew.executeQuery("select * from testOld");
        connOldViaNew.close();
        connOldViaNew2.close();
        assertTrue(new File("data/test/upgradeOld.data.db").exists());

        // Create new DB
        Connection connNew = DriverManager.getConnection("jdbc:h2:data/test/upgrade" + additionalParameters);
        Connection connNew2 = DriverManager.getConnection("jdbc:h2:data/test/upgrade" + additionalParameters);
        Statement statNew = connNew.createStatement();
        statNew.execute("create table test(id int)");
        // Link to old DB without upgrade
        statNew.executeUpdate("CREATE LOCAL TEMPORARY LINKED TABLE linkedTestOld('org.h2.Driver', 'jdbc:h2:data/test/upgradeOld;NO_UPGRADE=TRUE" + additionalParameters + "', '', '', 'TestOld')");
        statNew.executeQuery("select * from linkedTestOld");
        connNew.close();
        connNew2.close();
        assertTrue(new File("data/test/upgradeOld.data.db").exists());
        assertTrue(new File("data/test/upgrade.h2.db").exists());

        connNew = DriverManager.getConnection("jdbc:h2:data/test/upgrade" + additionalParameters);
        connNew2 = DriverManager.getConnection("jdbc:h2:data/test/upgrade" + additionalParameters);
        statNew = connNew.createStatement();
        // Link to old DB with upgrade
        statNew.executeUpdate("CREATE LOCAL TEMPORARY LINKED TABLE linkedTestOld('org.h2.Driver', 'jdbc:h2:data/test/upgradeOld" + additionalParameters + "', '', '', 'TestOld')");
        statNew.executeQuery("select * from linkedTestOld");
        connNew.close();
        connNew2.close();
        assertTrue(new File("data/test/upgradeOld.h2.db").exists());
        assertTrue(new File("data/test/upgrade.h2.db").exists());

        deleteDb("upgrade");
        deleteDb("upgradeOld");
    }

    private void testIfExists() throws Exception {
        deleteDb("upgrade");

        // Create old db
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.load");
        Connection connOld = DriverManager.getConnection("jdbc:h2v1_1:data/test/upgrade;PAGE_STORE=FALSE");
        // Test auto server, too
        Connection connOld2 = DriverManager.getConnection("jdbc:h2v1_1:data/test/upgrade;PAGE_STORE=FALSE");
        Statement statOld = connOld.createStatement();
        statOld.execute("create table test(id int)");
        connOld.close();
        connOld2.close();
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.unload");
        assertTrue(new File("data/test/upgrade.data.db").exists());

        // Connect to old DB with upgrade
        Connection connOldViaNew = DriverManager.getConnection("jdbc:h2:data/test/upgrade;ifexists=true");
        Statement statOldViaNew = connOldViaNew.createStatement();
        statOldViaNew.executeQuery("select * from test");
        connOldViaNew.close();
        assertTrue(new File("data/test/upgrade.h2.db").exists());

        deleteDb("upgrade");
    }

    public void deleteDb(String dbName) throws SQLException {
        super.deleteDb(dbName);
        try {
            Utils.callStaticMethod("org.h2.upgrade.v1_1.tools.DeleteDbFiles.execute", "data/test", dbName, true);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }
}