/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;

import org.h2.constant.SysProperties;
import org.h2.util.StringUtils;

/**
 * The compatibility modes. There is a fixed set of modes (for example
 * PostgreSQL, MySQL). Each mode has different settings.
 */
public class Mode {

    /**
     * The name of the default mode.
     */
    public static final String REGULAR = "REGULAR";

    private static final HashMap MODES = new HashMap();

    // Modes are also documented in the features section

    /**
     * Concatenation of a NULL with another value results in NULL. Usually, the
     * NULL is treated as an empty string if only one of the operators is NULL,
     * and NULL is only returned if both values are NULL.
     */
    public boolean nullConcatIsNull;

    /**
     * When inserting data, if a column is defined to be NOT NULL and NULL is
     * inserted, then a 0 (or empty string, or the current timestamp for
     * timestamp columns) value is used. Usually, this operation is not allowed
     * and an exception is thrown.
     */
    public boolean convertInsertNullToZero;

    /**
     * When converting the scale of decimal data, the number is only converted
     * if the new scale is smaller then current scale. Usually, the scale is
     * converted and 0s are added if required.
     */
    public boolean convertOnlyToSmallerScale;

    /**
     * When converting a floating point number to a integer, the fractional
     * digits should not be truncated, but the value should be rounded.
     */
    public boolean roundWhenConvertToLong;

    /**
     * The identifiers should be returned in lower case.
     */
    public boolean lowerCaseIdentifiers;

    /**
     * Creating indexes in the CREATE TABLE statement should be supported.
     */
    public boolean indexDefinitionInCreateTable;

    /**
     * The system columns 'CTID' and 'OID' should be supported.
     */
    public boolean systemColumns;

    /**
     * Identifiers may be quoted using square brackets as in [Test].
     */
    public boolean squareBracketQuotedNames;

    /**
     * When using unique indexes, multiple rows with NULL in one of the columns
     * are allowed by default. However many databases view NULL as distinct in
     * this regard and only allow one row with NULL.
     */
    public boolean uniqueIndexSingleNull;

    /**
     * When using unique indexes, multiple rows with NULL in all columns
     * are allowed, however it is not allowed to have multiple rows with the
     * same values otherwise. This is how Oracle works.
     */
    public boolean uniqueIndexSingleNullExceptAllColumnsAreNull;

    /**
     * If the syntax [OFFSET .. ROW] [FETCH ... ONLY] should be supported.
     * This is an alternative syntax for LIMIT .. OFFSET.
     */
    public boolean supportOffsetFetch;

    /**
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     */
    public boolean aliasColumnName;

    private String name;

    static {
        Mode mode = new Mode(REGULAR);
        mode.aliasColumnName = SysProperties.ALIAS_COLUMN_NAME;
        add(mode);

        mode = new Mode("PostgreSQL");
        mode.nullConcatIsNull = true;
        mode.roundWhenConvertToLong = true;
        mode.systemColumns = true;
        mode.aliasColumnName = true;
        add(mode);

        mode = new Mode("MySQL");
        mode.convertInsertNullToZero = true;
        mode.roundWhenConvertToLong = true;
        mode.lowerCaseIdentifiers = true;
        mode.indexDefinitionInCreateTable = true;
        add(mode);

        mode = new Mode("HSQLDB");
        mode.nullConcatIsNull = true;
        mode.convertOnlyToSmallerScale = true;
        mode.uniqueIndexSingleNull = true;
        mode.aliasColumnName = true;
        add(mode);

        mode = new Mode("MSSQLServer");
        mode.squareBracketQuotedNames = true;
        mode.uniqueIndexSingleNull = true;
        mode.aliasColumnName = true;
        add(mode);

        mode = new Mode("Derby");
        mode.uniqueIndexSingleNull = true;
        mode.aliasColumnName = true;
        add(mode);

        mode = new Mode("Oracle");
        mode.uniqueIndexSingleNullExceptAllColumnsAreNull = true;
        mode.aliasColumnName = true;
        add(mode);

        mode = new Mode("DB2");
        mode.supportOffsetFetch = true;
        mode.aliasColumnName = true;
        add(mode);

    }

    private Mode(String name) {
        this.name = name;
    }

    private static void add(Mode mode) {
        MODES.put(StringUtils.toUpperEnglish(mode.name), mode);
    }

    /**
     * Get the mode with the given name.
     *
     * @param name the name of the mode
     * @return the mode object
     */
    public static Mode getInstance(String name) {
        return (Mode) MODES.get(StringUtils.toUpperEnglish(name));
    }

    public String getName() {
        return name;
    }

}
