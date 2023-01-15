/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.scripts;

import java.sql.SQLException;

import org.h2.api.Aggregate;
import org.h2.api.H2Type;

/**
 * An aggregate function for tests.
 */
public class Aggregate1 implements Aggregate {

    @Override
    public int getInternalType(int[] inputTypes) throws SQLException {
        return H2Type.INTEGER.getVendorTypeNumber();
    }

    @Override
    public void add(Object value) throws SQLException {
    }

    @Override
    public Object getResult() throws SQLException {
        return 0;
    }

}
