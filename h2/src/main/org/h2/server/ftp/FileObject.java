/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileObject {
    boolean exists();
    boolean isDirectory();
    boolean isFile();
    boolean delete();
    boolean mkdirs();
    long lastModified();
    boolean renameTo(FileObject fileNew);
    void read(long skip, OutputStream out) throws IOException;
    FileObject[] listFiles();
    long length();
    boolean canRead();
    boolean canWrite();
    String getName();
    void write(InputStream in) throws IOException;
}
