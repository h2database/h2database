/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

public class MemoryFile {
    private String name;
    private int length;
    private byte[] data;
    private int pos;
    private byte[] magic;
    
    MemoryFile(String name) {
        this.name = name;
        data = new byte[16];
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public long length() {
        return length;
    }
    
    public void setLength(long l) {
        length = (int)l;
    }
    
    public void seek(long pos) {
        this.pos = (int)pos;
    }
    
    public void write(byte[] b, int off, int len) {
        if(pos+len > length) {
            length = pos+len;
        }
        if(pos+len > data.length) {
            byte[] n = new byte[length*2];
            System.arraycopy(data, 0, n, 0, data.length);
            data = n;
        }
        System.arraycopy(b, off, data, pos, len);
        pos += len;
    }

    public void readFully(byte[] b, int off, int len) {
        if(pos+len > length) {
            length = pos+len;
        }
        if(pos+len > data.length) {
            byte[] n = new byte[length*2];
            System.arraycopy(data, 0, n, 0, data.length);
            data = n;            
        }
        System.arraycopy(data, pos, b, off, len);
        pos += len;
    }
    
    public long getFilePointer() {
        return pos;
    }

    public void setMagic(byte[] magic) {
        this.magic = magic;
    }
    
    public byte[] getMagic() {
        return magic;
    }
}
