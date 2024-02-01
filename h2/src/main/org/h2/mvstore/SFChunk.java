/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Class SFChunk.
 * <UL>
 * <LI> 4/23/22 12:58 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
final class SFChunk extends Chunk<SFChunk>
{
    SFChunk(int id) {
        super(id);
    }

    SFChunk(String line) {
        super(line);
    }

    SFChunk(Map<String, String> map) {
        super(map, false);
    }

    @Override
    protected ByteBuffer readFully(FileStore<SFChunk> fileStore, long filePos, int length) {
        return fileStore.readFully(this, filePos, length);
    }
}
