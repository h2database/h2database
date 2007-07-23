/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.util;

import java.sql.SQLException;

/**
 * @author Thomas
 */

public interface CacheWriter {
    void writeBack(CacheObject entry) throws SQLException;
    void flushLog() throws SQLException;
}
