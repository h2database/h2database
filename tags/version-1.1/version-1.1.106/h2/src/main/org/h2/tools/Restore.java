/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.h2.message.Message;
import org.h2.store.FileLister;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.Tool;

/**
 * Restores a H2 database by extracting the database files from a .zip file.
 */
public class Restore extends Tool {

    private void showUsage() {
        out.println("Restores a database backup.");
        out.println("java "+getClass().getName() + "\n" +
                " [-file <filename>]  The source file name (default: backup.zip)\n" +
                " [-dir <dir>]        Target directory (default: .)\n" +
                " [-db <database>]    Target database name");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-db", "test",...
     * Options are case sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-file filename (the default is backup.zip)
     * </li><li>-dir database directory (the default is the current directory)
     * </li><li>-db database name (as stored in the backup if no name is specified)
     * </li></ul>
     *
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new Restore().run(args);
    }

    public void run(String[] args) throws SQLException {
        String zipFileName = "backup.zip";
        String dir = ".";
        String db = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-dir")) {
                dir = args[++i];
            } else if (arg.equals("-file")) {
                zipFileName = args[++i];
            } else if (arg.equals("-db")) {
                db = args[++i];
            } else if (arg.equals("-quiet")) {
                // ignore
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        process(zipFileName, dir, db);
    }

    private static String getOriginalDbName(String fileName, String db) throws IOException {
        InputStream in = null;
        try {
            in = FileUtils.openFileInputStream(fileName);
            ZipInputStream zipIn = new ZipInputStream(in);
            String originalDbName = null;
            boolean multiple = false;
            while (true) {
                ZipEntry entry = zipIn.getNextEntry();
                if (entry == null) {
                    break;
                }
                String entryName = entry.getName();
                zipIn.closeEntry();
                String name = FileLister.getDatabaseNameFromFileName(entryName);
                if (name != null) {
                    if (db.equals(name)) {
                        originalDbName = name;
                        // we found the correct database
                        break;
                    } else if (originalDbName == null) {
                        originalDbName = name;
                        // we found a database, but maybe another one
                    } else {
                        // we have found multiple databases, but not the correct
                        // one
                        multiple = true;
                    }
                }
            }
            zipIn.close();
            if (multiple && !originalDbName.equals(db)) {
                throw new IOException("Multiple databases found, but not " + db);
            }
            return originalDbName;
        } finally {
            IOUtils.closeSilently(in);
        }
    }

    /**
     * Restores database files.
     *
     * @param zipFileName the name of the backup file
     * @param directory the directory name
     * @param db the database name (null for all databases)
     * @param quiet don't print progress information
     * @throws SQLException
     */
    public static void execute(String zipFileName, String directory, String db, boolean quiet) throws SQLException {
        new Restore().process(zipFileName, directory, db);
    }

    /**
     * Restores database files.
     *
     * @param zipFileName the name of the backup file
     * @param directory the directory name
     * @param db the database name (null for all databases)
     * @param quiet don't print progress information
     * @throws SQLException
     */
    private void process(String zipFileName, String directory, String db) throws SQLException {
        InputStream in = null;
        try {
            if (!FileUtils.exists(zipFileName)) {
                throw new IOException("File not found: " + zipFileName);
            }
            String originalDbName = null;
            if (db != null) {
                originalDbName = getOriginalDbName(zipFileName, db);
                if (originalDbName == null) {
                    throw new IOException("No database named " + db + " found");
                }
                if (originalDbName.startsWith(File.separator)) {
                    originalDbName = originalDbName.substring(1);
                }
            }
            in = FileUtils.openFileInputStream(zipFileName);
            ZipInputStream zipIn = new ZipInputStream(in);
            while (true) {
                ZipEntry entry = zipIn.getNextEntry();
                if (entry == null) {
                    break;
                }
                String fileName = entry.getName();
                // restoring windows backups on linux and vice versa
                fileName = fileName.replace('\\', File.separatorChar);
                fileName = fileName.replace('/', File.separatorChar);
                if (fileName.startsWith(File.separator)) {
                    fileName = fileName.substring(1);
                }
                boolean copy = false;
                if (db == null) {
                    copy = true;
                } else if (fileName.startsWith(originalDbName)) {
                    fileName = db + fileName.substring(originalDbName.length());
                    copy = true;
                }
                if (copy) {
                    OutputStream out = null;
                    try {
                        out = FileUtils.openFileOutputStream(directory + File.separator + fileName, false);
                        IOUtils.copy(zipIn, out);
                        out.close();
                    } finally {
                        IOUtils.closeSilently(out);
                    }
                }
                zipIn.closeEntry();
            }
            zipIn.closeEntry();
            zipIn.close();
        } catch (IOException e) {
            throw Message.convertIOException(e, zipFileName);
        } finally {
            IOUtils.closeSilently(in);
        }
    }

}
