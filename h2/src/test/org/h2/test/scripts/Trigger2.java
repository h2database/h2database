/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.scripts;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.h2.api.Trigger;

/**
 * A trigger for tests.
 */
public class Trigger2 implements Trigger {

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, //
            int type) throws SQLException {
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        if (oldRow == null && newRow != null) {
            PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?, ?)");
            Long id = (Long) newRow[0];
            if (id != null) {
                prep.setLong(1, id);
            } else {
                prep.setNull(1, Types.BIGINT);
            }
            prep.setInt(2, (int) newRow[1]);
            prep.setInt(3, (int) newRow[2]);
            prep.executeUpdate();
        } else if (oldRow != null && newRow != null) {
            PreparedStatement prep = conn.prepareStatement("UPDATE TEST SET (ID, A, B) = (?, ?, ?) WHERE ID = ?");
            prep.setLong(1, (long) newRow[0]);
            prep.setInt(2, (int) newRow[1]);
            prep.setInt(3, (int) newRow[2]);
            prep.setLong(4, (long) oldRow[0]);
            prep.executeUpdate();
        } else if (oldRow != null && newRow == null) {
            PreparedStatement prep = conn.prepareStatement("DELETE FROM TEST WHERE ID = ?");
            prep.setLong(1, (long) oldRow[0]);
            prep.executeUpdate();
        }
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public void remove() throws SQLException {
    }

}
