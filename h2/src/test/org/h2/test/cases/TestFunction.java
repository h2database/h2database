/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.tools.SimpleResultSet;

public class TestFunction {

    public static ResultSet temp(Object[] values) throws SQLException{
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("TEMP1", java.sql.Types.INTEGER, 0, 0);
        for(int i = 0; values != null && i < values.length; i++){
         result.addRow(new Object[] {values[i]});
        }
        return result;
       }
}
