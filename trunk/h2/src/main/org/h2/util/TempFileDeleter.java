/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;

import org.h2.engine.Constants;
import org.h2.message.Message;

public class TempFileDeleter {

    private static ReferenceQueue queue = new ReferenceQueue();
    private static HashMap refMap = new HashMap();

    public static synchronized Reference addFile(String fileName, Object file) {
        PhantomReference ref = new PhantomReference(file, queue);
        refMap.put(ref, fileName);
        deleteUnused();
        return ref;
    }

    public static synchronized void deleteFile(Reference ref, String fileName) {
        if(ref != null) {
            String f2 = (String) refMap.remove(ref);
            if(Constants.CHECK && f2 != null && fileName != null && !f2.equals(fileName)) {
                throw Message.getInternalError("f2:"+f2+" f:"+fileName);
            }
        }
        if(fileName != null && FileUtils.exists(fileName)) {
            try {
                FileUtils.delete(fileName);
            } catch(Exception e) {
                // TODO log such errors?
            }
        }
        deleteUnused();
    }
    
    public static void deleteUnused() {
        while(true) {
            Reference ref = queue.poll();
            if(ref == null) {
                break;
            }
            deleteFile(ref, null);
        }
    }

    public static void stopAutoDelete(Reference ref, String fileName) {
        if(ref != null) {
            String f2 = (String) refMap.remove(ref);
            if(Constants.CHECK && (f2 == null || !f2.equals(fileName))) {
                throw Message.getInternalError("f2:"+f2+" f:"+fileName);
            }
        }
        deleteUnused();
    }

}
