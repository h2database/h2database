/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.constraint.Constraint.Type;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends AlterTable {

    private String constraintName;
    private final boolean ifExists;
    private ConstraintActionType dropAction;

    public AlterTableDropConstraint(SessionLocal session, Schema schema, boolean ifExists) {
        super(session, schema);
        this.ifExists = ifExists;
        dropAction = getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT : ConstraintActionType.CASCADE;
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public long update(Table table) {
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        Type constraintType;
        if (constraint == null || (constraintType = constraint.getConstraintType()) == Type.DOMAIN
                || constraint.getTable() != table) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
            }
        } else {
            Table refTable = constraint.getRefTable();
            if (refTable != table) {
                session.getUser().checkTableRight(refTable, Right.SCHEMA_OWNER);
            }
            if (constraintType.isUnique()) {
                for (Constraint c : constraint.getTable().getConstraints()) {
                    if (c.getReferencedConstraint() == constraint) {
                        if (dropAction == ConstraintActionType.RESTRICT) {
                            throw DbException.get(ErrorCode.CONSTRAINT_IS_USED_BY_CONSTRAINT_2,
                                    constraint.getTraceSQL(), c.getTraceSQL());
                        }
                        Table t = c.getTable();
                        if (t != table && t != refTable) {
                            session.getUser().checkTableRight(t, Right.SCHEMA_OWNER);
                        }
                    }
                }
            }
            getDatabase().removeSchemaObject(session, constraint);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_DROP_CONSTRAINT;
    }

}
