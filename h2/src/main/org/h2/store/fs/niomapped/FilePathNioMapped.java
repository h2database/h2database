/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.niomapped;

import java.io.IOException;
import java.nio.channels.FileChannel;
import org.h2.store.fs.FilePathWrapper;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class used memory mapped files.
 */
public class FilePathNioMapped extends FilePathWrapper {

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNioMapped(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "nioMapped";
    }

}
