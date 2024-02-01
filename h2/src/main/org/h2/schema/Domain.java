/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.util.ArrayList;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.message.Trace;
import org.h2.table.ColumnTemplate;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Represents a domain.
 */
public final class Domain extends SchemaObject implements ColumnTemplate {

    private TypeInfo type;

    /**
     * Parent domain.
     */
    private Domain domain;

    private Expression defaultExpression;

    private Expression onUpdateExpression;

    private ArrayList<ConstraintDomain> constraints;

    public Domain(Schema schema, int id, String name) {
        super(schema, id, name, Trace.SCHEMA);
    }

    @Override
    public String getDropSQL() {
        StringBuilder builder = new StringBuilder("DROP DOMAIN IF EXISTS ");
        return getSQL(builder, DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public String getCreateSQL() {
        StringBuilder builder = getSQL(new StringBuilder("CREATE DOMAIN "), DEFAULT_SQL_FLAGS).append(" AS ");
        if (domain != null) {
            domain.getSQL(builder, DEFAULT_SQL_FLAGS);
        } else {
            type.getSQL(builder, DEFAULT_SQL_FLAGS);
        }
        if (defaultExpression != null) {
            defaultExpression.getUnenclosedSQL(builder.append(" DEFAULT "), DEFAULT_SQL_FLAGS);
        }
        if (onUpdateExpression != null) {
            onUpdateExpression.getUnenclosedSQL(builder.append(" ON UPDATE "), DEFAULT_SQL_FLAGS);
        }
        return builder.toString();
    }

    public void setDataType(TypeInfo type) {
        this.type = type;
    }

    public TypeInfo getDataType() {
        return type;
    }

    @Override
    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    @Override
    public Domain getDomain() {
        return domain;
    }

    @Override
    public void setDefaultExpression(SessionLocal session, Expression defaultExpression) {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = ValueExpression.get(defaultExpression.getValue(session));
            }
        }
        this.defaultExpression = defaultExpression;
    }

    @Override
    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    @Override
    public Expression getEffectiveDefaultExpression() {
        return defaultExpression != null ? defaultExpression
                : domain != null ? domain.getEffectiveDefaultExpression() : null;
    }

    @Override
    public String getDefaultSQL() {
        return defaultExpression == null ? null
                : defaultExpression.getUnenclosedSQL(new StringBuilder(), DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public void setOnUpdateExpression(SessionLocal session, Expression onUpdateExpression) {
        // also to test that no column names are used
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
            if (onUpdateExpression.isConstant()) {
                onUpdateExpression = ValueExpression.get(onUpdateExpression.getValue(session));
            }
        }
        this.onUpdateExpression = onUpdateExpression;
    }

    @Override
    public Expression getOnUpdateExpression() {
        return onUpdateExpression;
    }

    @Override
    public Expression getEffectiveOnUpdateExpression() {
        return onUpdateExpression != null ? onUpdateExpression
                : domain != null ? domain.getEffectiveOnUpdateExpression() : null;
    }

    @Override
    public String getOnUpdateSQL() {
        return onUpdateExpression == null ? null
                : onUpdateExpression.getUnenclosedSQL(new StringBuilder(), DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public void prepareExpressions(SessionLocal session) {
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
        }
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
        }
        if (domain != null) {
            domain.prepareExpressions(session);
        }
    }

    /**
     * Add a constraint to the domain.
     *
     * @param constraint the constraint to add
     */
    public void addConstraint(ConstraintDomain constraint) {
        if (constraints == null) {
            constraints = Utils.newSmallArrayList();
        }
        if (!constraints.contains(constraint)) {
            constraints.add(constraint);
        }
    }

    public ArrayList<ConstraintDomain> getConstraints() {
        return constraints;
    }

    /**
     * Remove the given constraint from the list.
     *
     * @param constraint the constraint to remove
     */
    public void removeConstraint(Constraint constraint) {
        if (constraints != null) {
            constraints.remove(constraint);
        }
    }

    @Override
    public int getType() {
        return DbObject.DOMAIN;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        if (constraints != null && !constraints.isEmpty()) {
            for (ConstraintDomain constraint : constraints.toArray(new ConstraintDomain[0])) {
                database.removeSchemaObject(session, constraint);
            }
            constraints = null;
        }
        database.removeMeta(session, getId());
    }

    /**
     * Check the specified value.
     *
     * @param session the session
     * @param value the value
     */
    public void checkConstraints(SessionLocal session, Value value) {
        if (constraints != null) {
            for (ConstraintDomain constraint : constraints) {
                constraint.check(session, value);
            }
        }
        if (domain != null) {
            domain.checkConstraints(session, value);
        }
    }

}
