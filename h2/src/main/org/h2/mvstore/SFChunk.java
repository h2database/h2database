/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Map;

/**
 * Class SFChunk.
 * <UL>
 * <LI> 4/23/22 12:58 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
class SFChunk extends Chunk
{
    SFChunk(int id) {
        super(id);
    }

    SFChunk(String line) {
        super(line);
    }

    SFChunk(Map<String, String> map) {
        this(map, false);
    }

    SFChunk(Map<String, String> map, boolean full) {
        super(map, full);
    }


}
