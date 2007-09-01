/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileObjectDatabase implements FileObject {

    static FileObjectDatabase get(FileSystemDatabase db, String name) {
        return new FileObjectDatabase(db, name);
    }
    
    private FileSystemDatabase db;
    private String fullName;
    
    private FileObjectDatabase(FileSystemDatabase db, String fullName) {
        this.db = db;
        this.fullName = fullName;
    }

    public boolean canRead() {
        return true;
    }

    public boolean canWrite() {
        return true;
    }

    public boolean delete() {
        db.delete(fullName);
        return true;
    }

    public boolean exists() {
        return db.exists(fullName);
    }

    public void read(long skip, OutputStream out) throws IOException {
        db.read(fullName, skip, out);
    }

    public String getName() {
        return db.getName(fullName);
    }

    public void write(InputStream in) throws IOException {
        db.write(fullName, in);
    }

    public boolean isDirectory() {
        return db.isDirectory(fullName);
    }

    public boolean isFile() {
        return !db.isDirectory(fullName);
    }

    public long lastModified() {
        return db.lastModified(fullName);
    }

    public long length() {
        return db.length(fullName);
    }

    public FileObject[] listFiles() {
        return db.listFiles(fullName);
    }

    public boolean mkdirs() {
        db.mkdirs(fullName);
        return true;
    }

    public boolean renameTo(FileObject fileNew) {
        return db.renameTo(fullName, ((FileObjectDatabase) fileNew).fullName);
    }

}
