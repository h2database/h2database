/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class uses FileChannel.
 */
public class FilePathNio extends FilePathWrapper {

    @Override
    public FileChannel open(String mode) throws IOException {
        return FileChannel.open(Paths.get(name.substring(getScheme().length() + 1)),
                FileUtils.modeToOptions(mode), FileUtils.NO_ATTRIBUTES);
    }

    @Override
    public String getScheme() {
        return "nio";
    }

}
