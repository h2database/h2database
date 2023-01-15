/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.scripts;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

/**
 * A trigger for tests.
 */
public class Trigger1 implements Trigger {

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        if (newRow != null) {
            newRow[2] = ((int) newRow[2]) * 10;
        }
    }

}
