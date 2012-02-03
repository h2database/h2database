/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import java.util.ArrayList;
import org.h2.engine.Constants;
import org.h2.util.FileUtils;
import org.h2.util.New;

/**
 * Utility class to list the files of a database.
 */
public class FileLister {

    private FileLister() {
        // utility class
    }

    /**
     * Extract the name of the database from a given file name.
     * Only files ending with .data.db are considered, all others return null.
     *
     * @param fileName the file name (without directory)
     * @return the database name or null
     */
    public static String getDatabaseNameFromFileName(String fileName) {
        if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            return fileName.substring(0, fileName.length() - Constants.SUFFIX_PAGE_FILE.length());
        } else if (fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
            return fileName.substring(0, fileName.length() - Constants.SUFFIX_DATA_FILE.length());
        }
        return null;
    }

    /**
     * Get the list of database files.
     *
     * @param dir the directory (null for the current directory)
     * @param db the database name (null for all databases)
     * @param all  if true, files such as the lock, trace, hash index, and lob
     *            files are included. If false, only data, index and log files
     *            are returned
     * @return the list of files
     */
    public static ArrayList<String> getDatabaseFiles(String dir, String db, boolean all) throws SQLException {
        if (dir == null || dir.equals("")) {
            dir = ".";
        }
        dir = FileUtils.normalize(dir);
        ArrayList<String> files = New.arrayList();
        String start = db == null ? null : FileUtils.normalize(dir + "/" + db);
        String[] list = FileUtils.listFiles(dir);
        for (int i = 0; list != null && i < list.length; i++) {
            String f = list[i];
            boolean ok = false;
            if (f.endsWith(Constants.SUFFIX_DATA_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_INDEX_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_LOG_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_LOBS_DIRECTORY)) {
                if (start == null || FileUtils.fileStartsWith(f, start + ".")) {
                    files.addAll(getDatabaseFiles(f, null, all));
                    ok = true;
                }
            } else if (f.endsWith(Constants.SUFFIX_LOB_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_PAGE_FILE)) {
                ok = true;
            } else if (all) {
                if (f.endsWith(Constants.SUFFIX_LOCK_FILE)) {
                    ok = true;
                } else if (f.endsWith(Constants.SUFFIX_TEMP_FILE)) {
                    ok = true;
                } else if (f.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    ok = true;
                }
            }
            if (ok) {
                if (db == null || FileUtils.fileStartsWith(f, start + ".")) {
                    String fileName = f;
                    files.add(fileName);
                }
            }
        }
        return files;
    }

}
