/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;


public class SessionState {
    int sessionId;
    int lastCommitLog;
    int lastCommitPos;
    InDoubtTransaction inDoubtTransaction;

    public boolean isCommitted(int logId, int pos) {
        if (logId != lastCommitLog) {
            return lastCommitLog > logId;
        }
        return lastCommitPos >= pos;
    }

    public String toString() {
        return "sessionId:" + sessionId + " log:" + lastCommitLog + " pos:" + lastCommitPos + " inDoubt:" + inDoubtTransaction;
    }
}
