/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.DosFileAttributeView;

public abstract class Windows {
    private Windows() {
    }

    /**
     * True if this test is executed on the Microsoft Windows platform
     */
    public static final boolean IS_WINDOWS = StringUtils.toLowerEnglish(Utils.getProperty("os.name", ""))
            .startsWith("windows");

    /**
     * Enable the ability to write and delete a file on DOS FS if it exists
     *
     * @param fileName the file name
     */
    public static void setWritableIfExists(String fileName) throws IOException {
        final Path f = Paths.get(fileName);
        if (Files.exists(f)) {
            final FileStore fileStore = Files.getFileStore(f);
            fileStore.supportsFileAttributeView(DosFileAttributeView.class);
            Files.setAttribute(f, "dos:readonly", false);
        }
    }
}
