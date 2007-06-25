/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.command.dml.Select;
import org.h2.value.Value;

public interface ColumnResolver {

    String getTableAlias();
    Column[] getColumns();
    String getSchemaName();
    Value getValue(Column column) throws SQLException;
    TableFilter getTableFilter();
    Select getSelect();

}
