/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class is responsible to read resources and generate the
 * ResourceData.java file from the resources.
 */
public class Resources {

    private static final HashMap<String, byte[]> FILES = New.hashMap();

    private Resources() {
        // utility class
    }

    static {
        loadFromZip();
    }

    private static void loadFromZip() {
        InputStream in = Resources.class.getResourceAsStream("data.zip");
        if (in == null) {
            return;
        }
        ZipInputStream zipIn = new ZipInputStream(in);
        try {
            while (true) {
                ZipEntry entry = zipIn.getNextEntry();
                if (entry == null) {
                    break;
                }
                String entryName = entry.getName();
                if (!entryName.startsWith("/")) {
                    entryName = "/" + entryName;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtils.copy(zipIn, out);
                zipIn.closeEntry();
                FILES.put(entryName, out.toByteArray());
            }
            zipIn.close();
        } catch (IOException e) {
            // if this happens we have a real problem
            e.printStackTrace();
        }
    }

    /**
     * Get a resource from the resource map.
     *
     * @param name the name of the resource
     * @return the resource data
     */
    public static byte[] get(String name) throws IOException {
        byte[] data;
        if (FILES.size() == 0) {
            // TODO web: security (check what happens with files like 'lpt1.txt' on windows)
            InputStream in = Resources.class.getResourceAsStream(name);
            if (in == null) {
                data = null;
            } else {
                data = IOUtils.readBytesAndClose(in, 0);
            }
        } else {
            data = FILES.get(name);
        }
        return data == null ? MemoryUtils.EMPTY_BYTES : data;
    }

}
