/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends SchemaCommand {

    private String constraintName;
    private final boolean ifExists;
    private ConstraintActionType dropAction;

    public AlterTableDropConstraint(Session session, Schema schema, boolean ifExists) {
        super(session, schema);
        this.ifExists = ifExists;
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT : ConstraintActionType.CASCADE;
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int update() {
        session.commit(true);
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        Type constraintType;
        if (constraint == null || (constraintType = constraint.getConstraintType()) == Type.DOMAIN) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
            }
        } else {
            session.getUser().checkRight(constraint.getTable(), Right.ALL);
            session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
            if (constraintType == Type.PRIMARY_KEY || constraintType == Type.UNIQUE) {
                for (Constraint c : constraint.getTable().getConstraints()) {
                    if (c.getReferencedConstraint() == constraint) {
                        if (dropAction == ConstraintActionType.RESTRICT) {
                            throw DbException.get(ErrorCode.CONSTRAINT_IS_USED_BY_CONSTRAINT_2,
                                    constraint.getTraceSQL(), c.getTraceSQL());
                        }
                        session.getUser().checkRight(c.getTable(), Right.ALL);
                    }
                }
            }
            session.getDatabase().removeSchemaObject(session, constraint);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_DROP_CONSTRAINT;
    }

}
