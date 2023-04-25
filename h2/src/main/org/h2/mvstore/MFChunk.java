/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Class MFChunk.
 * <UL>
 * <LI> 4/23/22 12:49 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
final class MFChunk extends Chunk<MFChunk>
{
    private static final String ATTR_VOLUME = "vol";

    /**
     * The index of the file (0-based), containing this chunk.
     */
    public volatile int volumeId;

    MFChunk(int id) {
        super(id);
    }

    MFChunk(String line) {
        super(line);
    }

    MFChunk(Map<String, String> map) {
        super(map, false);
        volumeId = DataUtils.readHexInt(map, ATTR_VOLUME, 0);
    }

    @Override
    protected ByteBuffer readFully(FileStore<MFChunk> fileStore, long filePos, int length) {
        return fileStore.readFully(this, filePos, length);
    }

    @Override
    protected void dump(StringBuilder buff) {
        super.dump(buff);
        if (volumeId != 0) {
            DataUtils.appendMap(buff, ATTR_VOLUME, volumeId);
        }
    }
}
