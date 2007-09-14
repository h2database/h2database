/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.util.FileUtils;
import org.h2.util.MemoryFile;

/**
 * This class is an abstraction of an in-memory file.
 * A {@link MemoryFile} contains the actual bytes.
 */
public class MemoryFileStore extends FileStore {

    private MemoryFile memFile;

    public MemoryFileStore(DataHandler handler, String name, byte[] magic) throws SQLException {
        super(handler, magic);
        this.name = name;
        memFile = FileUtils.getMemoryFile(name);
        memFile.setMagic(magic);
    }

    public void close() {
        memFile.close();
    }
    
    public long getFilePointer() {
        return memFile.getFilePointer();
    }    
    
    public long length() {
        return memFile.length();
    }    
    
    public void readFully(byte[] b, int off, int len) throws SQLException {
        checkPowerOff();
        memFile.readFully(b, off, len);
    }    
    
    public void seek(long pos) {
        memFile.seek(pos);
    }    
    
    public void setLength(long newLength) throws SQLException {
        checkPowerOff();
        memFile.setLength(newLength);
    }    
    
    public void write(byte[] b, int off, int len) throws SQLException {
        checkPowerOff();
        memFile.write(b, off, len);
    }    
    
    public void closeFile() throws IOException {
        memFile.close();
    }

    public void openFile() throws IOException {
        memFile.open();
    }    

}
