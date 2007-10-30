/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;


public class FileInputStream extends InputStream {
    
    private FileObject file;
    private byte[] buffer = new byte[1];
    
    FileInputStream(FileObject file) {
        this.file = file;
    }

    public int read() throws IOException {
        if (file.getFilePointer() >= file.length()) {
            return -1;
        }
        file.readFully(buffer, 0, 1);
        return buffer[0];
    }
    
    private int todoWriteFtpTest;
    private int todoReadBlock;

    public void close() throws IOException {
        file.close();
    }

}
