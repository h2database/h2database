/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.command.Parser;
import org.h2.engine.Mode.ExpressionNames;
import org.h2.engine.Session;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.message.DbException;
import org.h2.util.HasSQL;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * Simple general value specifications.
 */
public final class CurrentGeneralValueSpecification extends Operation0 {

    /**
     * The "CURRENT_CATALOG" general value specification.
     */
    public static final int CURRENT_CATALOG = 0;

    /**
     * The "CURRENT_PATH" general value specification.
     */
    public static final int CURRENT_PATH = CURRENT_CATALOG + 1;

    /**
     * The function "CURRENT_ROLE" general value specification.
     */
    public static final int CURRENT_ROLE = CURRENT_PATH + 1;

    /**
     * The function "CURRENT_SCHEMA" general value specification.
     */
    public static final int CURRENT_SCHEMA = CURRENT_ROLE + 1;

    /**
     * The function "CURRENT_USER" general value specification.
     */
    public static final int CURRENT_USER = CURRENT_SCHEMA + 1;

    /**
     * The function "SESSION_USER" general value specification.
     */
    public static final int SESSION_USER = CURRENT_USER + 1;

    /**
     * The function "SYSTEM_USER" general value specification.
     */
    public static final int SYSTEM_USER = SESSION_USER + 1;

    private static final String[] NAMES = { "CURRENT_CATALOG", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA",
            "CURRENT_USER", "SESSION_USER", "SYSTEM_USER" };

    /**
     * Get the name for this specification id.
     *
     * @param specification
     *            the specification id
     * @return the name
     */
    public static String getName(int specification) {
        return NAMES[specification];
    }

    private final int specification;

    public CurrentGeneralValueSpecification(int specification) {
        this.specification = specification;
    }

    @Override
    public Value getValue(Session session) {
        String s;
        switch (specification) {
        case CURRENT_CATALOG:
            s = session.getDatabase().getShortName();
            break;
        case CURRENT_PATH: {
            String[] searchPath = session.getSchemaSearchPath();
            if (searchPath != null) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < searchPath.length; i++) {
                    if (i > 0) {
                        builder.append(',');
                    }
                    Parser.quoteIdentifier(builder, searchPath[i], HasSQL.DEFAULT_SQL_FLAGS);
                }
                s = builder.toString();
            } else {
                s = "";
            }
            break;
        }
        case CURRENT_ROLE:
            s = session.getDatabase().getPublicRole().getName();
            break;
        case CURRENT_SCHEMA:
            s = session.getCurrentSchemaName();
            break;
        case CURRENT_USER:
        case SESSION_USER:
        case SYSTEM_USER:
            s = session.getUser().getName();
            break;
        default:
            throw DbException.throwInternalError("specification=" + specification);
        }
        return s != null ? ValueVarchar.get(s, session) : ValueNull.INSTANCE;
    }

    @Override
    public String getAlias(Session session, int columnIndex) {
        if (session.getMode().expressionNames == ExpressionNames.POSTGRESQL_STYLE) {
            return StringUtils.toLowerEnglish(NAMES[specification]);
        }
        return super.getAlias(session, columnIndex);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(NAMES[specification]);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        }
        return true;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_VARCHAR;
    }

    @Override
    public int getCost() {
        return 1;
    }

}
