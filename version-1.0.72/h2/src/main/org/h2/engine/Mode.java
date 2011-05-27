/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;

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

    public boolean nullConcatIsNull;
    public boolean convertInsertNullToZero;
    public boolean convertOnlyToSmallerScale;
    public boolean roundWhenConvertToLong;
    public boolean lowerCaseIdentifiers;
    public boolean indexDefinitionInCreateTable;
    public boolean systemColumns;
    public boolean squareBracketQuotedNames;

    private static final HashMap MODES = new HashMap();

    private String name;

    static {
        Mode mode = new Mode(REGULAR);
        add(mode);

        mode = new Mode("PostgreSQL");
        mode.nullConcatIsNull = true;
        mode.roundWhenConvertToLong = true;
        mode.systemColumns = true;
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
        add(mode);

        mode = new Mode("MSSQLServer");
        mode.squareBracketQuotedNames = true;
        add(mode);

    }

    private static void add(Mode mode) {
        MODES.put(StringUtils.toUpperEnglish(mode.name), mode);
    }

    private Mode(String name) {
        this.name = name;
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
