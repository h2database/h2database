/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf.context;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;

import org.h2.engine.SysProperties;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Contains meta data information about a database schema.
 * This class is used by the H2 Console.
 */
public class DbSchema {

    private static final String COLUMNS_QUERY_H2_197 = "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE "
            + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?1 AND TABLE_NAME = ?2";

    private static final String COLUMNS_QUERY_H2_202 = "SELECT COLUMN_NAME, ORDINAL_POSITION, "
            + "DATA_TYPE_SQL(?1, ?2, 'TABLE', ORDINAL_POSITION) COLUMN_TYPE "
            + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?1 AND TABLE_NAME = ?2";

    /**
     * The schema name.
     */
    public final String name;

    /**
     * True if this is the default schema for this database.
     */
    public final boolean isDefault;

    /**
     * True if this is a system schema (for example the INFORMATION_SCHEMA).
     */
    public final boolean isSystem;

    /**
     * The quoted schema name.
     */
    public final String quotedName;

    /**
     * The database content container.
     */
    private final DbContents contents;

    /**
     * The table list.
     */
    private DbTableOrView[] tables;

    /**
     * The procedures list.
     */
    private DbProcedure[] procedures;

    DbSchema(DbContents contents, String name, boolean isDefault) {
        this.contents = contents;
        this.name = name;
        this.quotedName = contents.quoteIdentifier(name);
        this.isDefault = isDefault;
        if (name == null) {
            // firebird
            isSystem = true;
        } else if ("INFORMATION_SCHEMA".equalsIgnoreCase(name)) {
            isSystem = true;
        } else if (!contents.isH2() &&
                StringUtils.toUpperEnglish(name).startsWith("INFO")) {
            isSystem = true;
        } else if (contents.isPostgreSQL() &&
                StringUtils.toUpperEnglish(name).startsWith("PG_")) {
            isSystem = true;
        } else if (contents.isDerby() && name.startsWith("SYS")) {
            isSystem = true;
        } else {
            isSystem = false;
        }
    }

    /**
     * @return The database content container.
     */
    public DbContents getContents() {
        return contents;
    }

    /**
     * @return The table list.
     */
    public DbTableOrView[] getTables() {
        return tables;
    }

    /**
     * @return The procedure list.
     */
    public DbProcedure[] getProcedures() {
        return procedures;
    }

    /**
     * Read all tables for this schema from the database meta data.
     *
     * @param meta the database meta data
     * @param tableTypes the table types to read
     * @throws SQLException on failure
     */
    public void readTables(DatabaseMetaData meta, String[] tableTypes)
            throws SQLException {
        ResultSet rs = meta.getTables(null, name, null, tableTypes);
        ArrayList<DbTableOrView> list = new ArrayList<>();
        while (rs.next()) {
            DbTableOrView table = new DbTableOrView(this, rs);
            if (contents.isOracle() && table.getName().indexOf('$') > 0) {
                continue;
            }
            list.add(table);
        }
        rs.close();
        tables = list.toArray(new DbTableOrView[0]);
        if (tables.length < SysProperties.CONSOLE_MAX_TABLES_LIST_COLUMNS) {
            try (PreparedStatement ps = contents.isH2() ? prepareColumnsQueryH2(meta.getConnection()) : null) {
                for (DbTableOrView tab : tables) {
                    try {
                        tab.readColumns(meta, ps);
                    } catch (SQLException e) {
                        // MySQL:
                        // View '...' references invalid table(s) or column(s)
                        // or function(s) or definer/invoker of view
                        // lack rights to use them HY000/1356
                        // ignore
                    }
                }
            }
        }
    }

    private static PreparedStatement prepareColumnsQueryH2(Connection connection) throws SQLException {
        try {
            return connection.prepareStatement(COLUMNS_QUERY_H2_202);
        } catch (SQLSyntaxErrorException ex) {
            return connection.prepareStatement(COLUMNS_QUERY_H2_197);
        }
    }

    /**
     * Read all procedures in the database.
     *
     * @param meta the database meta data
     * @throws SQLException Error while fetching procedures
     */
    public void readProcedures(DatabaseMetaData meta) throws SQLException {
        ResultSet rs = meta.getProcedures(null, name, null);
        ArrayList<DbProcedure> list = Utils.newSmallArrayList();
        while (rs.next()) {
            list.add(new DbProcedure(this, rs));
        }
        rs.close();
        procedures = list.toArray(new DbProcedure[0]);
        if (procedures.length < SysProperties.CONSOLE_MAX_PROCEDURES_LIST_COLUMNS) {
            for (DbProcedure procedure : procedures) {
                procedure.readParameters(meta);
            }
        }
    }
}
