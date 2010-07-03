/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.upgrade;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.h2.message.DbException;
import org.h2.util.Utils;

/**
 * Class to convert a 1.1 DB (non page store) to a 1.2 DB (page store) format.
 * Conversion is done via "script to" and "runscript from".
 */
public class DbUpgradeNonPageStoreToCurrent {

    private static boolean scriptInTmpDir = false;
    private static boolean deleteOldDb = false;

    private String url;
    private Properties info;

    private boolean mustBeConverted;
    private String newName;
    private String oldUrl;
    private File oldDataFile;
    private File oldIndexFile;
    private File newFile;
    private File backupDataFile;
    private File backupIndexFile;

    private boolean successful;

    /**
     * Converts a database from a 1.1 DB (non page store) to a 1.2 DB
     * (page store) format.
     *
     * @param url The connection string
     * @param info The connection properties
     * @throws SQLException if an exception occurred
     */
    public DbUpgradeNonPageStoreToCurrent(String url, Properties info) throws SQLException {
        this.url = url;
        this.info = info;
        init();
    }

    private void init() throws SQLException {
        try {
            String oldStartUrlPrefix = (String) Utils.getStaticField("org.h2.upgrade.v1_1_to_v1_2.engine.Constants.START_URL");
            oldUrl = url;
            oldUrl = oldUrl.replaceAll(org.h2.engine.Constants.START_URL, oldStartUrlPrefix);
            oldUrl = oldUrl.replaceAll(";IGNORE_UNKNOWN_SETTINGS=TRUE", "");
            oldUrl = oldUrl.replaceAll(";IGNORE_UNKNOWN_SETTINGS=FALSE", "");
            oldUrl = oldUrl.replaceAll(";IFEXISTS=TRUE", "");
            oldUrl = oldUrl.replaceAll(";IFEXISTS=FALSE", "");
            oldUrl += ";IGNORE_UNKNOWN_SETTINGS=TRUE";
            Object ci = Utils.newInstance("org.h2.upgrade.v1_1_to_v1_2.engine.ConnectionInfo", oldUrl, info);
            boolean isRemote = (Boolean) Utils.callMethod("isRemote", ci);
            boolean isPersistent = (Boolean) Utils.callMethod("isPersistent", ci);
            String dbName = (String) Utils.callMethod("getName", ci);
            if (!isRemote && isPersistent) {
                String oldDataName = dbName + ".data.db";
                String oldIndexName = dbName + ".index.db";
                newName = dbName + ".h2.db";
                oldDataFile = new File(oldDataName).getAbsoluteFile();
                oldIndexFile = new File(oldIndexName).getAbsoluteFile();
                newFile = new File(newName).getAbsoluteFile();
                backupDataFile = new File(oldDataFile.getAbsolutePath() + ".backup");
                backupIndexFile = new File(oldIndexFile.getAbsolutePath() + ".backup");
                mustBeConverted = oldDataFile.exists() && !newFile.exists();
            }
        } catch (Exception e) {
            DbException.toSQLException(e);
        }
    }


    /**
     * Returns if a database must be converted by this class.
     *
     * @param url The connection string
     * @param info The connection properties
     * @return if the conversion classes were found and the database must be
     *         converted
     * @throws SQLException
     */
    public boolean mustBeConverted(String url, Properties info) throws SQLException {
        return mustBeConverted;
    }

    /**
     * Converts the database from 1.1 (non page store) to current (page store).
     *
     * @param url The connection string
     * @param info The connection properties
     * @throws SQLException
     */
    public void upgrade(String url, Properties info) throws SQLException {
        successful = true;
        if (!mustBeConverted) {
            return;
        }
        File scriptFile = null;
        try {
            if (scriptInTmpDir) {
                scriptFile = File.createTempFile("h2dbmigration", "backup.sql");
            } else {
                scriptFile = new File(oldDataFile.getAbsolutePath() + "_script.sql");
            }

//            outputStream.println("H2 Migrating '" + oldFile.getPath() +
//            "' to '" + newFile.getPath() + "' via '" + scriptFile.getPath()
//            + "'");

            Utils.callStaticMethod("org.h2.upgrade.v1_1_to_v1_2.Driver.load");
            Connection connection = DriverManager.getConnection(oldUrl, info);
            Statement stmt = connection.createStatement();
            stmt.execute("script to '" + scriptFile + "'");
            stmt.close();
            connection.close();

            oldDataFile.renameTo(backupDataFile);
            oldIndexFile.renameTo(backupIndexFile);

            connection = DriverManager.getConnection(url, info);
            stmt = connection.createStatement();
            stmt.execute("runscript from '" + scriptFile + "'");
            stmt.close();
            connection.close();

            if (deleteOldDb) {
                backupDataFile.delete();
                backupIndexFile.delete();
            }

//            outputStream.println("H2 Migration of '" + oldFile.getPath() +
//            "' finished successfully");
        } catch (Exception e)  {
            successful = false;
            if (backupDataFile.exists()) {
                backupDataFile.renameTo(oldDataFile);
            }
            if (backupIndexFile.exists()) {
                backupIndexFile.renameTo(oldIndexFile);
            }
            newFile.delete();
//            errorStream.println("H2 Migration of '" + oldFile.getPath() +
//            "' finished with error: " + e.getMessage());
            throw DbException.toSQLException(e);
        } finally {
            if (scriptFile != null) {
                scriptFile.delete();
            }
        }
    }

    /**
     * Returns if the database upgrade was successful.
     *
     * @return if the database upgrade was successful
     */
    public boolean wasSuccessful() {
        return successful;
    }

    /**
     * The conversion script file will per default be created in the db
     * directory. Use this method to change the directory to the temp
     * directory.
     *
     * @param scriptInTmpDir true if the conversion script should be
     *        located in the temp directory.
     */
    public static void setScriptInTmpDir(boolean scriptInTmpDir) {
        DbUpgradeNonPageStoreToCurrent.scriptInTmpDir = scriptInTmpDir;
    }

    /**
     * Old files will be renamed to .backup after a successful conversion. To
     * delete them after the conversion, use this method with the parameter
     * 'true'.
     *
     * @param deleteOldDb if true, the old db files will be deleted.
     */
    public static void setDeleteOldDb(boolean deleteOldDb) {
        DbUpgradeNonPageStoreToCurrent.deleteOldDb = deleteOldDb;
    }

}

