/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

/**
 * Represents a record in the transaction log.
 */
class LogRecord {

    /**
     * The log file of this record.
     */
    LogFile log;

    /**
     * The position in the log file.
     */
    int logRecordId;

    /**
     * The session id of this record.
     */
    int sessionId;

    LogRecord(LogFile log, int logRecordId, int sessionId) {
        this.log = log;
        this.logRecordId = logRecordId;
        this.sessionId = sessionId;
    }
}
