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
import java.util.UUID;
import org.h2.message.DbException;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemDisk;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Class to convert a 1.1 DB (non page store) to a 1.2 DB (page store) format.
 * Conversion is done via "script to" and "runscript from".
 */
public class DbUpgradeNonPageStoreToCurrent {

    private static boolean scriptInTmpDir;
    private static boolean deleteOldDb;

    private String url;
    private Properties info;

    private boolean mustBeConverted;
    private String newName;
    private String newUrl;
    private String oldUrl;
    private File oldDataFile;
    private File oldIndexFile;
    private File oldLobsDir;
    private File newFile;
    private File backupDataFile;
    private File backupIndexFile;
    private File backupLobsDir;

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
            newUrl = url;
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";UNDO_LOG=1", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";UNDO_LOG=0", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOG=0", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOG=1", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOG=2", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOCK_MODE=0", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOCK_MODE=1", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOCK_MODE=2", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";LOCK_MODE=3", "");
            newUrl = StringUtils.replaceAllIgnoreCase(newUrl, ";IFEXISTS=TRUE", "");
            newUrl += ";UNDO_LOG=0;LOG=0;LOCK_MODE=0";
            String oldStartUrlPrefix = (String) Utils.getStaticField("org.h2.upgrade.v1_1.engine.Constants.START_URL");
            oldUrl = url;
            oldUrl = StringUtils.replaceAll(oldUrl, org.h2.engine.Constants.START_URL, oldStartUrlPrefix);
            oldUrl = StringUtils.replaceAllIgnoreCase(oldUrl, ";IGNORE_UNKNOWN_SETTINGS=TRUE", "");
            oldUrl = StringUtils.replaceAllIgnoreCase(oldUrl, ";IGNORE_UNKNOWN_SETTINGS=FALSE", "");
            oldUrl = StringUtils.replaceAllIgnoreCase(oldUrl, ";PAGE_STORE=TRUE", "");
            oldUrl += ";IGNORE_UNKNOWN_SETTINGS=TRUE";
            Object ci = Utils.newInstance("org.h2.upgrade.v1_1.engine.ConnectionInfo", oldUrl, info);
            boolean isRemote = (Boolean) Utils.callMethod(ci, "isRemote");
            boolean isPersistent = (Boolean) Utils.callMethod(ci, "isPersistent");
            String dbName = (String) Utils.callMethod(ci, "getName");
            // remove stackable file systems
            int colon = dbName.indexOf(':');
            while (colon != -1) {
                String fileSystemPrefix = dbName.substring(0, colon+1);
                FileSystem fs = FileSystem.getInstance(fileSystemPrefix);
                if (fs == null || fs instanceof FileSystemDisk) {
                    break;
                }
                dbName = dbName.substring(colon+1);
                colon = dbName.indexOf(':');
            }
            if (!isRemote && isPersistent) {
                String oldDataName = dbName + ".data.db";
                String oldIndexName = dbName + ".index.db";
                String oldLobsName = dbName + ".lobs.db";
                newName = dbName + ".h2.db";
                oldDataFile = new File(oldDataName).getAbsoluteFile();
                oldIndexFile = new File(oldIndexName).getAbsoluteFile();
                oldLobsDir = new File(oldLobsName).getAbsoluteFile();
                newFile = new File(newName).getAbsoluteFile();
                backupDataFile = new File(oldDataFile.getAbsolutePath() + ".backup");
                backupIndexFile = new File(oldIndexFile.getAbsolutePath() + ".backup");
                backupLobsDir = new File(oldLobsDir.getAbsolutePath() + ".backup");
                mustBeConverted = oldDataFile.exists() && !newFile.exists();
            }
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }


    /**
     * Returns if a database must be converted by this class.
     *
     * @return if the conversion classes were found and the database must be
     *         converted
     * @throws SQLException
     */
    public boolean mustBeConverted() throws SQLException {
        return mustBeConverted;
    }

    /**
     * Converts the database from 1.1 (non page store) to current (page store).
     *
     * @throws SQLException
     */
    public void upgrade() throws SQLException {
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

            Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.load");
            Connection connection = DriverManager.getConnection(oldUrl, info);
            Statement stmt = connection.createStatement();
            boolean isEncrypted = StringUtils.toUpperEnglish(url).indexOf(";CIPHER=") >= 0;
            String uuid = UUID.randomUUID().toString();
            if (isEncrypted) {
                stmt.execute("script to '" + scriptFile + "' CIPHER AES PASSWORD '" + uuid + "' --hide--");
            } else {
                stmt.execute("script to '" + scriptFile + "'");
            }
            stmt.close();
            connection.close();

            oldDataFile.renameTo(backupDataFile);
            oldIndexFile.renameTo(backupIndexFile);
            oldLobsDir.renameTo(backupLobsDir);

            connection = DriverManager.getConnection(newUrl, info);
            stmt = connection.createStatement();
            if (isEncrypted) {
                stmt.execute("runscript from '" + scriptFile + "' CIPHER AES PASSWORD '" + uuid + "' --hide--");
            } else {
                stmt.execute("runscript from '" + scriptFile + "'");
            }
            stmt.execute("analyze");
            stmt.execute("shutdown compact");
            stmt.close();
            connection.close();

            if (deleteOldDb) {
                backupDataFile.delete();
                backupIndexFile.delete();
                FileSystem.getInstance(backupLobsDir.getAbsolutePath()).deleteRecursive(backupLobsDir.getAbsolutePath(), false);
            }
        } catch (Exception e)  {
            successful = false;
            if (backupDataFile.exists()) {
                backupDataFile.renameTo(oldDataFile);
            }
            if (backupIndexFile.exists()) {
                backupIndexFile.renameTo(oldIndexFile);
            }
            if (backupLobsDir.exists()) {
                backupLobsDir.renameTo(oldLobsDir);
            }
            newFile.delete();
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

