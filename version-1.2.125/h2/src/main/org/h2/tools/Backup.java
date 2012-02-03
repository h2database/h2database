/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
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
 * Creates a backup of a database.
 * @h2.resource
 */
public class Backup extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-file &lt;filename&gt;]</td>
     * <td>The target file name (default: backup.zip)</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The source directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>Source database; not required if there is only one</td></tr>
     * <tr><td>[-quiet]</td>
     * <td>Do not print progress information</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Backup().run(args);
    }

    public void run(String... args) throws SQLException {
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
                throwUnsupportedOption(arg);
            }
        }
        process(zipFileName, dir, db, quiet);
    }

    /**
     * Backs up database files.
     *
     * @param zipFileName the name of the target backup file (including path)
     * @param directory the source directory name
     * @param db the source database name (null if there is only one database)
     * @param quiet don't print progress information
     * @throws SQLException
     */
    public static void execute(String zipFileName, String directory, String db, boolean quiet) throws SQLException {
        new Backup().process(zipFileName, directory, db, quiet);
    }

    private void process(String zipFileName, String directory, String db, boolean quiet) throws SQLException {
        ArrayList<String> list = FileLister.getDatabaseFiles(directory, db, true);
        if (list.size() == 0) {
            if (!quiet) {
                printNoDatabaseFilesFound(directory, db);
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
            for (String fileName : list) {
                if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
                    base = FileUtils.getParent(fileName);
                    break;
                } else if (fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
                    base = FileUtils.getParent(fileName);
                    break;
                }
            }
            for (String fileName : list) {
                String f = FileUtils.getAbsolutePath(fileName);
                if (!f.startsWith(base)) {
                    Message.throwInternalError(f + " does not start with " + base);
                }
                if (FileUtils.isDirectory(fileName)) {
                    continue;
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
