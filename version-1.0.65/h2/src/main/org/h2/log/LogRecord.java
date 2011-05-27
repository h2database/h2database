/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

/**
 * Represents a record in the transaction log.
 */
public class LogRecord {
    LogFile log;
    int logRecordId;
    int sessionId;
    LogRecord(LogFile log, int logRecordId, int sessionId) {
        this.log = log;
        this.logRecordId = logRecordId;
        this.sessionId = sessionId;
    }
}
