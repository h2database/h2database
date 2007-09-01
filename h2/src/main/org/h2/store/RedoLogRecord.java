/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

public class RedoLogRecord {
    Storage storage;
    int sequenceId;
    int recordId;
    int offset;
    byte[] data;
    
    public int getSize() {
        // estimated memory size in bytes ((5 variables+myself) * 4 bytes each)
        if (data == null) {
            return 24;
        } else {
            return 28 + data.length;
        }
    }
}
