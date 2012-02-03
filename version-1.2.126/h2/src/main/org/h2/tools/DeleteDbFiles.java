/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.engine.Constants;
import org.h2.store.FileLister;
import org.h2.store.fs.FileSystem;
import org.h2.util.FileUtils;
import org.h2.util.Tool;

/**
 * Deletes all files belonging to a database.
 * <br />
 * The database must be closed before calling this tool.
 * @h2.resource
 */
public class DeleteDbFiles extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>The database name</td></tr>
     * <tr><td>[-quiet]</td>
     * <td>Do not print progress information</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new DeleteDbFiles().run(args);
    }

    public void run(String... args) throws SQLException {
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
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                throwUnsupportedOption(arg);
            }
        }
        process(dir, db, quiet);
    }

    /**
     * Deletes the database files.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     * @param quiet don't print progress information
     * @throws SQLException
     */
    public static void execute(String dir, String db, boolean quiet) throws SQLException {
        new DeleteDbFiles().process(dir, db, quiet);
    }

    /**
     * Deletes the database files.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     * @param quiet don't print progress information
     * @throws SQLException
     */
    private void process(String dir, String db, boolean quiet) throws SQLException {
        DeleteDbFiles delete = new DeleteDbFiles();
        ArrayList<String> files = FileLister.getDatabaseFiles(dir, db, true);
        if (files.size() == 0 && !quiet) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (String fileName : files) {
            delete.process(fileName, quiet);
            if (!quiet) {
                out.println("Processed: " + fileName);
            }
        }
    }

    private void process(String fileName, boolean quiet) throws SQLException {
        if (FileUtils.isDirectory(fileName)) {
            FileSystem.getInstance(fileName).deleteRecursive(fileName, quiet);
        } else if (quiet || fileName.endsWith(Constants.SUFFIX_TEMP_FILE) || fileName.endsWith(Constants.SUFFIX_TRACE_FILE)) {
            FileUtils.tryDelete(fileName);
        } else {
            FileUtils.delete(fileName);
        }
    }

}
