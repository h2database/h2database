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
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER DOMAIN RENAME CONSTRAINT
 */
public class AlterDomainRenameConstraint extends AlterDomain {

    private String constraintName;
    private String newConstraintName;

    public AlterDomainRenameConstraint(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    public void setNewConstraintName(String newName) {
        this.newConstraintName = newName;
    }

    @Override
    long update(Schema schema, Domain domain) {
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        if (constraint == null || constraint.getConstraintType() != Type.DOMAIN
                || ((ConstraintDomain) constraint).getDomain() != domain) {
            throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
        }
        if (getSchema().findConstraint(session, newConstraintName) != null
                || newConstraintName.equals(constraintName)) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, newConstraintName);
        }
        getDatabase().renameSchemaObject(session, constraint, newConstraintName);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_RENAME_CONSTRAINT;
    }

}
