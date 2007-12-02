/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;

/**
 * This interface represents a RandomAccessFile.
 */
public interface FileObject {

    long length() throws IOException;

    void close() throws IOException;

    void readFully(byte[] b, int off, int len) throws IOException;

    void seek(long pos) throws IOException;

    void write(byte[] b, int off, int len) throws IOException;

    long getFilePointer() throws IOException;
    
    void sync() throws IOException;
    
    void setLength(long newLength) throws IOException;

}
