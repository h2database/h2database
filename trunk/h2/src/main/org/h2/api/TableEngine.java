/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import org.h2.table.Table;
import org.h2.command.ddl.CreateTableData;

/**
 * A class that implements this interface can create custom table
 * implementations.
 *
 * @author Sergi Vladykin
 */
public interface TableEngine {

    /**
     * Create new table.
     *
     * @param data the data to construct the table
     * @return the created table
     */
    Table createTable(CreateTableData data);

}
