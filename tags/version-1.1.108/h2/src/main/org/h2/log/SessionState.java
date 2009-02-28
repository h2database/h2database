/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

/**
 * The session state contains information about when was the last commit of a
 * session. It is only used during recovery.
 */
public class SessionState {

    /**
     * The session id
     */
    public int sessionId;

    /**
     * The last log file id where a commit for this session is found.
     */
    public int lastCommitLog;

    /**
     * The position where a commit for this session is found.
     */
    public int lastCommitPos;

    /**
     * The in-doubt transaction if there is one.
     */
    public InDoubtTransaction inDoubtTransaction;

    /**
     * Check if this session state is already committed at this point.
     *
     * @param logId the log file id
     * @param pos the position in the log file
     * @return true if it is committed
     */
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
