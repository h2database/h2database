/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.command.Parser;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * Keeps meta data information about a database.
 * This class is used by the H2 Console.
 */
public class DbContents {

    /**
     * The list of schemas.
     */
    DbSchema[] schemas;

    /**
     * The default schema.
     */
    DbSchema defaultSchema;

    /**
     * True if this is an Oracle database.
     */
    boolean isOracle;

    /**
     * True if this is a H2 database.
     */
    boolean isH2;

    /**
     * True if this is a H2 database in MySQL mode.
     */
    boolean isH2ModeMySQL;

    /**
     * True if this is a PostgreSQL database.
     */
    boolean isPostgreSQL;

    /**
     * True if this is a MySQL database.
     */
    boolean isMySQL;

    /**
     * True if this is an Apache Derby database.
     */
    boolean isDerby;

    /**
     * True if this is a Firebird database.
     */
    boolean isFirebird;

    /**
     * True if this is an SQLite database.
     */
    boolean isSQLite;

    /**
     * Get the column index of a column in a result set. If the column is not
     * found, the default column index is returned.
     * This is a workaround for a JDBC-ODBC bridge problem.
     *
     * @param rs the result set
     * @param columnName the column name
     * @param defaultColumnIndex the default column index
     * @return the column index
     */
    static int findColumn(ResultSet rs, String columnName, int defaultColumnIndex) {
        try {
            return rs.findColumn(columnName);
        } catch (SQLException e) {
            return defaultColumnIndex;
        }
    }

    /**
     * Read the contents of this database from the database meta data.
     *
     * @param meta the database meta data
     */
    void readContents(DatabaseMetaData meta) throws SQLException {
        String prod = StringUtils.toLowerEnglish(meta.getDatabaseProductName());
        isSQLite = prod.indexOf("sqlite") >= 0;
        String url = meta.getURL();
        if (url != null) {
            isH2 = url.startsWith("jdbc:h2:");
            if (isH2) {
                ResultSet rs = meta.getConnection().createStatement().executeQuery(
                        "SELECT UPPER(VALUE) FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME='MODE'");
                rs.next();
                if ("MYSQL".equals(rs.getString(1))) {
                    isH2ModeMySQL = true;
                }
            }
            isOracle = url.startsWith("jdbc:oracle:");
            isPostgreSQL = url.startsWith("jdbc:postgresql:");
            // isHSQLDB = url.startsWith("jdbc:hsqldb:");
            isMySQL = url.startsWith("jdbc:mysql:");
            isDerby = url.startsWith("jdbc:derby:");
            isFirebird = url.startsWith("jdbc:firebirdsql:");
        }
        String defaultSchemaName = getDefaultSchemaName(meta);
        String[] schemaNames = getSchemaNames(meta);
        schemas = new DbSchema[schemaNames.length];
        for (int i = 0; i < schemaNames.length; i++) {
            String schemaName = schemaNames[i];
            boolean isDefault = defaultSchemaName == null || defaultSchemaName.equals(schemaName);
            DbSchema schema = new DbSchema(this, schemaName, isDefault);
            if (schema.isDefault) {
                defaultSchema = schema;
            }
            schemas[i] = schema;
            String[] tableTypes = new String[] { "TABLE", "SYSTEM TABLE", "VIEW", "SYSTEM VIEW",
                    "TABLE LINK", "SYNONYM" };
            schema.readTables(meta, tableTypes);
        }
        if (defaultSchema == null) {
            String best = null;
            for (DbSchema schema : schemas) {
                if ("dbo".equals(schema.name)) {
                    // MS SQL Server
                    defaultSchema = schema;
                    break;
                }
                if (defaultSchema == null || best == null || schema.name.length() < best.length()) {
                    best = schema.name;
                    defaultSchema = schema;
                }
            }
        }
    }

    private String[] getSchemaNames(DatabaseMetaData meta) throws SQLException {
        if (isMySQL) {
            return new String[] { "" };
        } else if (isFirebird) {
            return new String[] { null };
        }
        ResultSet rs = meta.getSchemas();
        ArrayList<String> schemas = New.arrayList();
        while (rs.next()) {
            String schema = rs.getString(findColumn(rs, "TABLE_SCHEM", 1));
            if (isOracle) {
                for (String ignore : new String[] {
                        "CTXSYS", "DIP", "DBSNMP", "DMSYS", "EXFSYS", "MDDATA", "MDSYS",
                        "MGMT_VIEW", "OLAPSYS", "ORDSYS", "ORDPLUGINS", "OUTLN", "SI_INFORMTN_SCHEMA", "SYS", "SYSMAN",
                        "SYSTEM", "TSMSYS", "WMSYS", "XDB"
                }) {
                    if (ignore.equals(schema)) {
                        schema = null;
                        break;
                    }
                }
            }
            if (schema == null) {
                continue;
            }
            schemas.add(schema);
        }
        rs.close();
        String[] list = new String[schemas.size()];
        schemas.toArray(list);
        return list;
    }

    private String getDefaultSchemaName(DatabaseMetaData meta) {
        String defaultSchemaName = "";
        try {
            if (isOracle) {
                return meta.getUserName();
            } else if (isPostgreSQL) {
                return "public";
            } else if (isMySQL) {
                return "";
            } else if (isDerby) {
                return StringUtils.toUpperEnglish(meta.getUserName());
            } else if (isFirebird) {
                return null;
            }
            ResultSet rs = meta.getSchemas();
            int index = rs.findColumn("IS_DEFAULT");
            while (rs.next()) {
                if (rs.getBoolean(index)) {
                    defaultSchemaName = rs.getString("TABLE_SCHEM");
                }
            }
        } catch (SQLException e) {
            // IS_DEFAULT not found
        }
        return defaultSchemaName;
    }

    /**
     * Add double quotes around an identifier if required.
     * For the H2 database, all identifiers are quoted.
     *
     * @param identifier the identifier
     * @return the quoted identifier
     */
    String quoteIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        if (isH2 && !isH2ModeMySQL) {
            return Parser.quoteIdentifier(identifier);
        }
        return StringUtils.toUpperEnglish(identifier);
    }

}
