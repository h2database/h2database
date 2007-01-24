/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;

import org.h2.value.Value;

public interface DataHandler {
    boolean getTextStorage();
    String getDatabasePath();
    FileStore openFile(String name, boolean mustExist) throws SQLException;
    int getChecksum(byte[] data, int start, int end);
    void checkPowerOff() throws SQLException;
    void checkWritingAllowed() throws SQLException;
    void freeUpDiskSpace() throws SQLException;
    void handleInvalidChecksum() throws SQLException;
    int compareTypeSave(Value a, Value b) throws SQLException;
    int getMaxLengthInplaceLob();
    
    String getLobCompressionAlgorithm(int type);
    
    // only temporarily, until LOB_FILES_IN_DIRECTORIES is enabled
    int allocateObjectId(boolean needFresh, boolean dataFile);
    String createTempFile() throws SQLException;

}
