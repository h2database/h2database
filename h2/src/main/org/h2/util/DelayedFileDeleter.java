/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * Deletes files later on or if they are not used.
 * This class deletes temporary files when they are not used any longer.
 */
public class DelayedFileDeleter extends Thread {

    private static DelayedFileDeleter instance;

    private final ReferenceQueue queue = new ReferenceQueue();
    private final HashMap refMap = new HashMap();
    private final HashMap deleteLater = new HashMap();
    private long deleteNext;

    public synchronized void deleteLater(String fileName) throws SQLException {
        int delay = SysProperties.getLogFileDeleteDelay();
        if (delay == 0 && deleteLater.size() == 0) {
            // shortcut if delay is 0
            FileUtils.delete(fileName);
            return;
        }
        long at = System.currentTimeMillis() + delay;
        if (deleteNext != 0 && at <= deleteNext) {
            // make sure files are deleted in the correct order
            at = deleteNext + 1;
        }
        deleteNext = at;
        deleteLater.put(fileName, ObjectUtils.getLong(at));
    }

    /**
     * Delete at most one old file (after the delay)
     */
    private void deleteOld() {
        long now = System.currentTimeMillis();
        if (deleteNext == 0 || now < deleteNext) {
            return;
        }
        String delete = null;
        long oldest = 0;
        for (Iterator it = deleteLater.entrySet().iterator(); it.hasNext();) {
            Entry entry = (Entry) it.next();
            long at = ((Long) entry.getValue()).longValue();
            if (at < now && (delete == null || at < oldest)) {
                delete = (String) entry.getKey();
                oldest = at;
            }
        }
        if (delete == null) {
            return;
        }
        try {
            FileUtils.delete(delete);
        } catch (SQLException e) {
            // ignore
        }
        deleteLater.remove(delete);
    }

    public static synchronized DelayedFileDeleter getInstance() {
        if (instance == null) {
int test;
//System.out.println("DelayerFileDeleter.getInstance()");
            instance = new DelayedFileDeleter();
            instance.setDaemon(true);
            instance.setPriority(Thread.MIN_PRIORITY);
            instance.start();
        }
        return instance;
    }

    private DelayedFileDeleter() {
        setName("H2 FileDeleter");
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
            synchronized (this) {
                if (refMap.size() != 0) {
                    deleteUnused();
                } else if (deleteLater.size() != 0) {
                    deleteOld();
                } else {
                    break;
                }
            }
        }
int test;
//System.out.println("DelayerFileDeleter.stop()");
    }

    /**
     * Add a temp file to the queue and delete it if it is no longer referenced.
     *
     * @param fileName the file name
     * @param file the object to track
     * @return the reference
     */
    public synchronized Reference addTempFile(String fileName, Object file) {
        FileUtils.trace("FileDeleter.addFile", fileName, file);
        PhantomReference ref = new PhantomReference(file, queue);
        refMap.put(ref, fileName);
        return ref;
    }

    /**
     * Delete a file now and remove it from the queue.
     *
     * @param the reference in the queue
     * @param fileName the file name
     */
    public synchronized void autoDeleteFile(Reference ref, String fileName) {
        if (ref != null) {
            String f2 = (String) refMap.remove(ref);
            if (SysProperties.CHECK && f2 != null && fileName != null && !f2.equals(fileName)) {
                throw Message.getInternalError("f2:" + f2 + " f:" + fileName);
            }
        }
        if (fileName != null && FileUtils.exists(fileName)) {
            try {
                FileUtils.trace("FileDeleter.deleteFile", fileName, null);
                FileUtils.delete(fileName);
            } catch (Exception e) {
                // TODO log such errors?
            }
            deleteUnused();
        }
    }

    /**
     * Delete all unreferenced files that have been garbage collected now.
     * This method is called from time to time by the application.
     */
    private void deleteUnused() {
        while (true) {
            Reference ref = queue.poll();
            if (ref == null) {
                break;
            }
            autoDeleteFile(ref, null);
        }
    }

    /**
     * Remove a file from the list of files to be deleted.
     *
     * @param ref the reference
     * @param fileName the file name
     */
    public synchronized void stopAutoDelete(Reference ref, String fileName) {
        FileUtils.trace("FileDeleter.stopAutoDelete", fileName, ref);
        if (ref != null) {
            String f2 = (String) refMap.remove(ref);
            if (SysProperties.CHECK && (f2 == null || !f2.equals(fileName))) {
                throw Message.getInternalError("f2:" + f2 + " f:" + fileName);
            }
        }
    }

}
