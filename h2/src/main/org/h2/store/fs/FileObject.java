/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;

/**
 * This interface represents a random access file.
 */
public interface FileObject {

    /**
     * Get the length of the file.
     *
     * @return the length
     */
    long length() throws IOException;

    /**
     * Close the file.
     */
    void close() throws IOException;

    /**
     * Read from the file.
     * @param b the byte array
     * @param off the offset
     * @param len the number of bytes
     */
    void readFully(byte[] b, int off, int len) throws IOException;

    /**
     * Go to the specified position in the file.
     *
     * @param pos the new position
     */
    void seek(long pos) throws IOException;

    /**
     * Write to the file.
     *
     * @param b the byte array
     * @param off the offset
     * @param len the number of bytes
     */
    void write(byte[] b, int off, int len) throws IOException;

    /**
     * Get the file pointer.
     *
     * @return the current file pointer
     */
    long getFilePointer() throws IOException;

    /**
     * Force changes to the physical location.
     */
    void sync() throws IOException;

    /**
     * Change the length of the file.
     *
     * @param newLength the new length
     */
    void setFileLength(long newLength) throws IOException;

    /**
     * Get the full qualified name of this file.
     *
     * @return the name
     */
    String getName();
}
