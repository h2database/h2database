/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import org.h2.table.RecreatableTable;
import org.h2.command.ddl.CreateTableData;

/**
 * Class for creating custom table implementations
 * 
 * @author Sergi Vladykin
 */
public interface TableEngine {
    
    /**
     * Create new table
     * @param data for table construction
     * @return created table
     */
    RecreatableTable createTable(CreateTableData data); 
    
}
