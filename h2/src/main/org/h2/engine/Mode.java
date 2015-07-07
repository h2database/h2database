/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * The compatibility modes. There is a fixed set of modes (for example
 * PostgreSQL, MySQL). Each mode has different settings.
 */
public class Mode {

    /**
     * The name of the default mode.
     */
    static final String REGULAR = "REGULAR";

    private static final HashMap<String, Mode> MODES = New.hashMap();

    // Modes are also documented in the features section

    /**
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     */
    public boolean aliasColumnName;

    /**
     * When inserting data, if a column is defined to be NOT NULL and NULL is
     * inserted, then a 0 (or empty string, or the current timestamp for
     * timestamp columns) value is used. Usually, this operation is not allowed
     * and an exception is thrown.
     */
    public boolean convertInsertNullToZero;

    /**
     * When converting the scale of decimal data, the number is only converted
     * if the new scale is smaller than the current scale. Usually, the scale is
     * converted and 0s are added if required.
     */
    public boolean convertOnlyToSmallerScale;

    /**
     * Creating indexes in the CREATE TABLE statement is allowed using
     * <code>INDEX(..)</code> or <code>KEY(..)</code>.
     * Example: <code>create table test(id int primary key, name varchar(255),
     * key idx_name(name));</code>
     */
    public boolean indexDefinitionInCreateTable;

    /**
     * Meta data calls return identifiers in lower case.
     */
    public boolean lowerCaseIdentifiers;

    /**
     * Concatenation with NULL results in NULL. Usually, NULL is treated as an
     * empty string if only one of the operands is NULL, and NULL is only
     * returned if both operands are NULL.
     */
    public boolean nullConcatIsNull;

    /**
     * Identifiers may be quoted using square brackets as in [Test].
     */
    public boolean squareBracketQuotedNames;

    /**
     * Support for the syntax
     * [OFFSET .. ROW|ROWS] [FETCH FIRST .. ROW|ROWS ONLY]
     * as an alternative for LIMIT .. OFFSET.
     */
    public boolean supportOffsetFetch = Constants.VERSION_MINOR >= 4 ? true : false;

    /**
     * The system columns 'CTID' and 'OID' are supported.
     */
    public boolean systemColumns;

    /**
     * For unique indexes, NULL is distinct. That means only one row with NULL
     * in one of the columns is allowed.
     */
    public boolean uniqueIndexSingleNull;

    /**
     * When using unique indexes, multiple rows with NULL in all columns
     * are allowed, however it is not allowed to have multiple rows with the
     * same values otherwise.
     */
    public boolean uniqueIndexSingleNullExceptAllColumnsAreNull;

    /**
     * Empty strings are treated like NULL values. Useful for Oracle emulation.
     */
    public boolean treatEmptyStringsAsNull;

    /**
     * Support the pseudo-table SYSIBM.SYSDUMMY1.
     */
    public boolean sysDummy1;

    /**
     * Text can be concatenated using '+'.
     */
    public boolean allowPlusForStringConcat;

    /**
     * The function LOG() uses base 10 instead of E.
     */
    public boolean logIsLogBase10;

    /**
     * SERIAL and BIGSERIAL columns are not automatically primary keys.
     */
    public boolean serialColumnIsNotPK;

    /**
     * Swap the parameters of the CONVERT function.
     */
    public boolean swapConvertFunctionParameters;

    /**
     * can set the isolation level using WITH {RR|RS|CS|UR}
     */
    public boolean isolationLevelInSelectOrInsertStatement;

    /**
     * MySQL style INSERT ... ON DUPLICATE KEY UPDATE ...
     */
    public boolean onDuplicateKeyUpdate;

    /**
     * Support the # for column names
     */
    public boolean supportPoundSymbolForColumnNames;

    private final String name;

    static {
        Mode mode = new Mode(REGULAR);
        mode.nullConcatIsNull = true;
        add(mode);

        mode = new Mode("DB2");
        mode.aliasColumnName = true;
        mode.supportOffsetFetch = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        add(mode);

        mode = new Mode("Derby");
        mode.aliasColumnName = true;
        mode.uniqueIndexSingleNull = true;
        mode.supportOffsetFetch = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        add(mode);

        mode = new Mode("HSQLDB");
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.nullConcatIsNull = true;
        mode.uniqueIndexSingleNull = true;
        mode.allowPlusForStringConcat = true;
        add(mode);

        mode = new Mode("MSSQLServer");
        mode.aliasColumnName = true;
        mode.squareBracketQuotedNames = true;
        mode.uniqueIndexSingleNull = true;
        mode.allowPlusForStringConcat = true;
        mode.swapConvertFunctionParameters = true;
        mode.supportPoundSymbolForColumnNames = true;
        add(mode);

        mode = new Mode("MySQL");
        mode.convertInsertNullToZero = true;
        mode.indexDefinitionInCreateTable = true;
        mode.lowerCaseIdentifiers = true;
        mode.onDuplicateKeyUpdate = true;
        add(mode);

        mode = new Mode("Oracle");
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.uniqueIndexSingleNullExceptAllColumnsAreNull = true;
        mode.treatEmptyStringsAsNull = true;
        mode.supportPoundSymbolForColumnNames = true;
        add(mode);

        mode = new Mode("PostgreSQL");
        mode.aliasColumnName = true;
        mode.nullConcatIsNull = true;
        mode.supportOffsetFetch = true;
        mode.systemColumns = true;
        mode.logIsLogBase10 = true;
        mode.serialColumnIsNotPK = true;
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
        return MODES.get(StringUtils.toUpperEnglish(name));
    }

    public String getName() {
        return name;
    }

}
