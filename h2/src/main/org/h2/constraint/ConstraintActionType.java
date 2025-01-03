/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

/**
 * The referential action for update or delete rule.
 */
public enum ConstraintActionType {

    /**
     * No action (default).
     */
    NO_ACTION("NO ACTION"),

    /**
     * The action is to restrict the operation.
     */
    RESTRICT,

    /**
     * The action is to cascade the operation.
     */
    CASCADE,

    /**
     * The action is to set the value to the default value.
     */
    SET_DEFAULT("SET DEFAULT"),

    /**
     * The action is to set the value to NULL.
     */
    SET_NULL("SET NULL");

    private final String sqlName;

    private ConstraintActionType() {
        this.sqlName = name();
    }

    private ConstraintActionType(String sqlName) {
        this.sqlName = sqlName;
    }

    /**
     * Get standard SQL type name.
     *
     * @return standard SQL type name
     */
    public String getSqlName() {
        return sqlName;
    }

    /**
     * Tests if this is a {@link #NO_ACTION} or {@link #RESTRICT}
     * rule.
     *
     * @return {@code true} if this is a {@link #NO_ACTION} or {@link #RESTRICT}
     *         rule, {@code false} otherwise
     */
    public boolean isNoActionOrRestrict() {
        return this == NO_ACTION || this == RESTRICT;
    }

}
