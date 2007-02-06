/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.engine.Constants;
import org.h2.util.FileUtils;

/**
 * @author Thomas
 */

public class FileLister {
    
    /**
     * Get the list of database files.
     * 
     * @param dir the directory (null for the current directory)
     * @param db the database name (null for all databases)
     * @param all - if true, files such as the lock, trace, hash index, and lob files are included. If false, only data, index and log files are returned 
     * @return the list of files
     * @throws SQLException
     */
    public static ArrayList getDatabaseFiles(String dir, String db, boolean all) throws SQLException {
        ArrayList files = new ArrayList();
        if(dir == null || dir.equals("")) {
            dir = ".";
        }
        String start = db == null ? null : FileUtils.normalize(dir + "/" + db);
        String[] list = FileUtils.listFiles(dir);
        for(int i=0; list!=null && i<list.length; i++) {
            String f = list[i];
            boolean ok = false;
            if(f.endsWith(Constants.SUFFIX_DATA_FILE)) {
                ok = true;
            } else if(f.endsWith(Constants.SUFFIX_INDEX_FILE)) {
                ok = true;
            } else if(f.endsWith(Constants.SUFFIX_LOG_FILE)) {
                ok = true;
            } else if(f.endsWith(Constants.SUFFIX_HASH_FILE)) {
                ok = true;
            } else if(f.endsWith(Constants.SUFFIX_LOBS_DIRECTORY)) {
                files.addAll(getDatabaseFiles(f, null, all));
                ok = true;
            } else if(f.endsWith(Constants.SUFFIX_LOB_FILE)) {
                ok = true;
            } else if(all) {
                if(f.endsWith(Constants.SUFFIX_LOCK_FILE)) {
                    ok = true;
                } else if(f.endsWith(Constants.SUFFIX_TEMP_FILE)) {
                    ok = true;
                } else if(f.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    ok = true;
                }
            }
            if(ok) {
                if(db == null || FileUtils.fileStartsWith(f, start+".") || FileUtils.isInMemory(dir)) {
                    String fileName = f;
                    files.add(fileName);
                }
            }
        }
        return files;
    }

}
