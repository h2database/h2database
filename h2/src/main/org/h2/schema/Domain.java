/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import java.util.ArrayList;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * Represents a domain.
 */
public class Domain extends SchemaObjectBase {

    private Column column;

    private ArrayList<ConstraintDomain> constraints;

    public Domain(Schema schema, int id, String name) {
        super(schema, id, name, Trace.SCHEMA);
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        StringBuilder builder = new StringBuilder("DROP DOMAIN IF EXISTS ");
        return getSQL(builder, DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public String getCreateSQL() {
        return getSQL(new StringBuilder("CREATE DOMAIN "), DEFAULT_SQL_FLAGS).append(" AS ")
                .append(column.getCreateSQL()).toString();
    }

    public Column getColumn() {
        return column;
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
    public void removeChildrenAndResources(Session session) {
        if (constraints != null && !constraints.isEmpty()) {
            for (ConstraintDomain constraint : constraints.toArray(new ConstraintDomain[0])) {
                database.removeSchemaObject(session, constraint);
            }
            constraints = null;
        }
        database.removeMeta(session, getId());
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    /**
     * Check the specified value.
     *
     * @param session the session
     * @param value the value
     */
    public void checkConstraints(Session session, Value value) {
        if (constraints != null) {
            for (ConstraintDomain constraint : constraints) {
                constraint.check(session, value);
            }
        }
        Domain next = column.getDomain();
        if (next != null) {
            next.checkConstraints(session, value);
        }
    }

}
