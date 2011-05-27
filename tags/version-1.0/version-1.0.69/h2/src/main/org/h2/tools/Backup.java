/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.h2.command.dml.BackupCommand;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.store.FileLister;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.Tool;

/**
 * Backs up a H2 database by creating a .zip file from the database files.
 */
public class Backup extends Tool {
    
    private void showUsage() {
        out.println("Creates a backup of a database.");
        out.println("java "+getClass().getName() + "\n" +
            " [-file <filename>]  The target file name (default: backup.zip)\n" +
            " [-dir <dir>]        Source directory (default: .)\n" +
            " [-db <database>]    Source database name\n" +
            " [-quiet]            Do not print progress information");
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
     * </li><li>-db database name (not required if there is only one database)
     * </li><li>-quiet does not print progress information
     * </li></ul>
     *
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new Backup().run(args);
    }

    public void run(String[] args) throws SQLException {
        String zipFileName = "backup.zip";
        String dir = ".";
        String db = null;
        boolean quiet = false;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-dir")) {
                dir = args[++i];
            } else if (arg.equals("-db")) {
                db = args[++i];
            } else if (arg.equals("-quiet")) {
                quiet = true;
            } else if (arg.equals("-file")) {
                zipFileName = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        process(zipFileName, dir, db, quiet);
    }

    /**
     * Backs up database files.
     *
     * @param zipFileName the name of the backup file
     * @param directory the directory name
     * @param db the database name (null if there is only one database)
     * @param quiet don't print progress information
     * @throws SQLException
     */
    public static void execute(String zipFileName, String directory, String db, boolean quiet) throws SQLException {
        new Backup().process(zipFileName, directory, db, quiet);
    }
    
    private void process(String zipFileName, String directory, String db, boolean quiet) throws SQLException {
        ArrayList list = FileLister.getDatabaseFiles(directory, db, true);
        if (list.size() == 0) {
            if (!quiet) {
                out.println("No database files found");
            }
            return;
        }
        zipFileName = FileUtils.normalize(zipFileName);
        if (FileUtils.exists(zipFileName)) {
            FileUtils.delete(zipFileName);
        }
        OutputStream fileOut = null;
        try {
            fileOut = FileUtils.openFileOutputStream(zipFileName, false);
            ZipOutputStream zipOut = new ZipOutputStream(fileOut);
            String base = "";
            for (int i = 0; i < list.size(); i++) {
                String fileName = (String) list.get(i);
                if (fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
                    base = FileUtils.getParent(fileName);
                }
            }
            for (int i = 0; i < list.size(); i++) {
                String fileName = (String) list.get(i);
                String f = FileUtils.getAbsolutePath(fileName);
                if (!f.startsWith(base)) {
                    throw Message.getInternalError(f + " does not start with " + base);
                }
                f = f.substring(base.length());
                f = BackupCommand.correctFileName(f);
                ZipEntry entry = new ZipEntry(f);
                zipOut.putNextEntry(entry);
                InputStream in = null;
                try {
                    in = FileUtils.openFileInputStream(fileName);
                    IOUtils.copyAndCloseInput(in, zipOut);
                } catch (FileNotFoundException e) {
                    // the file could have been deleted in the meantime
                    // ignore this (in this case an empty file is created)
                } finally {
                    IOUtils.closeSilently(in);
                }
                zipOut.closeEntry();
                if (!quiet) {
                    out.println("Processed: " + fileName);
                }
            }
            zipOut.closeEntry();
            zipOut.close();
        } catch (IOException e) {
            throw Message.convertIOException(e, zipFileName);
        } finally {
            IOUtils.closeSilently(fileOut);
        }
    }

}
