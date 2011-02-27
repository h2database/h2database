/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import org.h2.jaqu.Table.JQTable;
import org.h2.jaqu.util.JdbcUtils;
import org.h2.jaqu.util.StringUtils;
import org.h2.jaqu.util.Utils;

/**
 * Class to inspect a model and a database for the purposes of model validation
 * and automatic model generation.  This class finds the available schemas and
 * tables and serves as the entry point for model generation and validation.
 */
public class DbInspector {

    private Db db;
    private DatabaseMetaData metaData;
    private Class<? extends java.util.Date> dateClass = java.util.Date.class;

    private int todoReviewWholeClass;

    public DbInspector(Db db) {
        this.db = db;
    }

    /**
     * Set the preferred Date class. Possible values are: java.util.Date
     * (default), java.sql.Date, java.sql.Timestamp.
     *
     * @param dateClass the new date class
     */
    public void setPreferredDateClass(Class<? extends java.util.Date> dateClass) {
        this.dateClass = dateClass;
    }

    /**
     * Generates models class skeletons for schemas and tables.  If the table
     * name is undefined, models will be generated for every table within the
     * specified schema.  Additionally, if no schema is defined, models will be
     * generated for all schemas and all tables.
     *
     * @param schema the schema name (optional)
     * @param table the table name (optional)
     * @param packageName the package name (optional)
     * @param annotateSchema (includes schema name in annotation)
     * @param trimStrings (trims strings to maxLength of column)
     * @return a list of complete model classes as strings, each element a class
     */
    public List<String> generateModel(String schema, String table,
            String packageName, boolean annotateSchema, boolean trimStrings) {
        try {
            List<String> models = Utils.newArrayList();
            List<TableInspector> tables = findTables(schema, table);
            for (TableInspector t : tables) {
                t.read(metaData);
                String model = t.generateModel(packageName, annotateSchema,
                        trimStrings);
                models.add(model);
            }
            return models;
        } catch (SQLException s) {
            throw new RuntimeException(s);
        }
    }

    /**
     * Validates a model.
     *
     * @param <T> the model class
     * @param model and instance of the model class
     * @param throwOnError
     * @return
     */
    public <T> List<ValidationRemark> validateModel(T model, boolean throwOnError) {
        try {
            TableInspector inspector = findTable(model);
            inspector.read(metaData);
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) model.getClass();
            TableDefinition<T> def = db.define(clazz);
            return inspector.validate(def, throwOnError);
        } catch (SQLException s) {
            throw new RuntimeException(s);
        }
    }

    private DatabaseMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = db.getConnection().getMetaData();
        }
        return metaData;
    }

    /**
     * Attempts to find a table in the database based on the model definition.
     *
     * @param <T>
     * @param model
     * @return
     * @throws SQLException
     */
    private <T> TableInspector findTable(T model) throws SQLException {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) model.getClass();
        TableDefinition<T> def = db.define(clazz);
        boolean forceUpperCase = getMetaData().storesUpperCaseIdentifiers();
        String schema = (forceUpperCase && def.schemaName != null) ?
                def.schemaName.toUpperCase() : def.schemaName;
        String table = forceUpperCase ? def.tableName.toUpperCase() : def.tableName;
        List<TableInspector> tables = findTables(schema, table);
        return tables.get(0);
    }

    /**
     * Returns a list of tables
     *
     * @param schema the schema name
     * @param table the table name
     * @return a list of table inspectors
     */
    private List<TableInspector> findTables(String schema, String table) throws SQLException {
        ResultSet rs = null;
        try {
            rs = getMetaData().getSchemas();
            ArrayList<String> schemaList = Utils.newArrayList();
            while (rs.next()) {
                schemaList.add(rs.getString("TABLE_SCHEM"));
            }
            JdbcUtils.closeSilently(rs);

            // get JaQu Tables table name.
            String jaquTables = DbVersion.class.getAnnotation(JQTable.class).name();

            List<TableInspector> tables = Utils.newArrayList();
            if (schemaList.size() == 0) {
                schemaList.add(null);
            }
            for (String s : schemaList) {
                rs = getMetaData().getTables(null, s, null, new String[] { "TABLE" });
                while (rs.next()) {
                    String t = rs.getString("TABLE_NAME");
                    if (!t.equalsIgnoreCase(jaquTables)) {
                        // Ignore JaQu versions table
                        tables.add(new TableInspector(s, t,
                            getMetaData().storesUpperCaseIdentifiers(), dateClass));
                    }
                }
            }

            if (StringUtils.isNullOrEmpty(schema) && StringUtils.isNullOrEmpty(table)) {
                // all schemas and tables
                return tables;
            }
            // schema subset OR table subset OR exact match
            List<TableInspector> matches = Utils.newArrayList();
            for (TableInspector t : tables) {
                if (t.matches(schema, table)) {
                    matches.add(t);
                }
            }
            if (matches.size() == 0) {
                throw new RuntimeException(
                        MessageFormat.format("Failed to find schema={0} table={1}",
                        schema == null ? "" : schema, table == null ? "" : table));
            }
            return matches;
        } finally {
            JdbcUtils.closeSilently(rs);
        }
    }

}
