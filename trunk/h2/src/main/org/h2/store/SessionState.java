/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

public class SessionState {
    int sessionId;
    int lastCommitLog;
    int lastCommitPos;
    InDoubtTransaction inDoubtTransaction;
    
    public boolean isCommitted(int logId, int pos) {
        if(logId != lastCommitLog) {
            return lastCommitLog > logId;
        }
        return lastCommitPos >= pos;
    }
}
