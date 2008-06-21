/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;

import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * This class deletes temporary files when they are not used any longer.
 */
public class TempFileDeleter {

    private static final ReferenceQueue QUEUE = new ReferenceQueue();
    private static final HashMap REF_MAP = new HashMap();
    
    private TempFileDeleter() {
        // utility class
    }
    
    /**
     * Contains information about a file.
     */
    static class TempFile {
        
        /**
         * The file name.
         */
        String fileName;
        
        /**
         * The last modified date of this file.
         */
        long lastModified;
    }

    /**
     * Add a file to the list of temp files to delete. The file is deleted once
     * the file object is garbage collected.
     * 
     * @param fileName the file name
     * @param file the object to monitor
     * @return the reference that can be used to stop deleting the file
     */
    public static synchronized Reference addFile(String fileName, Object file) {
        FileUtils.trace("TempFileDeleter.addFile", fileName, file);
        PhantomReference ref = new PhantomReference(file, QUEUE);
        TempFile f = new TempFile();
        f.fileName = fileName;
        f.lastModified = FileUtils.getLastModified(fileName);
        REF_MAP.put(ref, f);
        deleteUnused();
        return ref;
    }

    /**
     * Delete the given file now. This will remove the reference from the list.
     * 
     * @param ref the reference as returned by addFile
     * @param fileName the file name
     */
    public static synchronized void deleteFile(Reference ref, String fileName) {
        if (ref != null) {
            TempFile f2 = (TempFile) REF_MAP.remove(ref);
            if (f2 != null) {
                if (SysProperties.CHECK && fileName != null && !f2.fileName.equals(fileName)) {
                    throw Message.getInternalError("f2:" + f2.fileName + " f:" + fileName);
                }
                fileName = f2.fileName;
                long mod = FileUtils.getLastModified(fileName);
                if (mod != f2.lastModified) {
                    // the file has been deleted and a new one created
                    // or it has been modified afterwards
                    return;
                }
            }
        }
        if (fileName != null && FileUtils.exists(fileName)) {
            try {
                FileUtils.trace("TempFileDeleter.deleteFile", fileName, null);
                FileUtils.tryDelete(fileName);
            } catch (Exception e) {
                // TODO log such errors?
            }
        }
    }

    /**
     * Delete all unused files now.
     */
    public static void deleteUnused() {
        // Mystery: I don't know how QUEUE could get null, but two independent
        // people reported NullPointerException here - if somebody understands
        // how it could happen please report it!
        // Environment: web application under Tomcat, exception occurs during undeploy
        while (QUEUE != null) {
            Reference ref = QUEUE.poll();
            if (ref == null) {
                break;
            }
            deleteFile(ref, null);
        }
    }

    /**
     * This method is called if a file should no longer be deleted if the object
     * is garbage collected.
     * 
     * @param ref the reference as returned by addFile
     * @param fileName the file name
     */
    public static void stopAutoDelete(Reference ref, String fileName) {
        FileUtils.trace("TempFileDeleter.stopAutoDelete", fileName, ref);
        if (ref != null) {
            TempFile f2 = (TempFile) REF_MAP.remove(ref);
            if (SysProperties.CHECK && (f2 == null || !f2.fileName.equals(fileName))) {
                throw Message.getInternalError("f2:" + f2 + " " + (f2 == null ? "" : f2.fileName) + " f:" + fileName);
            }
        }
        deleteUnused();
    }

}
