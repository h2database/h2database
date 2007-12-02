/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.command.dml.Select;
import org.h2.value.Value;

/**
 * A column resolver is list of column (for example, a table) that can map a 
 * column name to an actual column.
 */
public interface ColumnResolver {

    String getTableAlias();
    Column[] getColumns();
    Column[] getSystemColumns();
    String getSchemaName();
    Value getValue(Column column) throws SQLException;
    TableFilter getTableFilter();
    Select getSelect();

}
