/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.engine.Constants;
import org.h2.store.FileLister;
import org.h2.util.FileUtils;

/**
 * Delete the database files. The database must be closed before calling this tool.
 * 
 * @author Thomas
 */

public class DeleteDbFiles {
    
    private void showUsage() {
        System.out.println("java "+getClass().getName()+" [-dir <dir>] [-db <database>] [-quiet]");
    }
    
    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-db", "test",... 
     * Options are case sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-dir database directory (the default is the current directory)
     * </li><li>-db database name (all databases if no name is specified)
     * </li><li>-quiet does not print progress information
     * </li></ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */    
    public static void main(String[] args) throws SQLException {
        new DeleteDbFiles().run(args);
    }

    private void run(String[] args) throws SQLException {
        String dir = ".";
        String db = null;
        boolean quiet = false;
        for (int i = 0; args != null && i < args.length; i++) {
            if (args[i].equals("-dir")) {
                dir = args[++i];
            } else if (args[i].equals("-db")) {
                db = args[++i];
            } else if (args[i].equals("-quiet")) {
                quiet = true;
            } else {
                showUsage();
                return;
            }
        }
        execute(dir, db, quiet);
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
        DeleteDbFiles delete = new DeleteDbFiles();
        ArrayList files = FileLister.getDatabaseFiles(dir, db, true);
        for (int i = 0; i < files.size(); i++) {
            String fileName = (String) files.get(i);
            delete.process(fileName, quiet);
            if (!quiet) {
                System.out.println("processed: " + fileName);
            }
        }
        if (files.size() == 0 && !quiet) {
            System.out.println("No database files found");
        }
    }

    private void process(String fileName, boolean quiet) throws SQLException {
        if (quiet || fileName.endsWith(Constants.SUFFIX_TEMP_FILE) || fileName.endsWith(Constants.SUFFIX_TRACE_FILE)) {
            FileUtils.tryDelete(fileName);
        } else {
            FileUtils.delete(fileName);
        }
    }  

}
