/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;

/**
 * This class represents the statement ALTER DOMAIN ADD CONSTRAINT
 */
public class AlterDomainAddConstraint extends AlterDomain {

    private String constraintName;
    private Expression checkExpression;
    private String comment;
    private boolean checkExisting;
    private final boolean ifNotExists;

    public AlterDomainAddConstraint(SessionLocal session, Schema schema, boolean ifNotExists) {
        super(session, schema);
        this.ifNotExists = ifNotExists;
    }

    private String generateConstraintName(Domain domain) {
        if (constraintName == null) {
            constraintName = getSchema().getUniqueDomainConstraintName(session, domain);
        }
        return constraintName;
    }

    @Override
    long update(Schema schema, Domain domain) {
        try {
            return tryUpdate(schema, domain);
        } finally {
            getSchema().freeUniqueName(constraintName);
        }
    }

    /**
     * Try to execute the statement.
     *
     * @param schema the schema
     * @param domain the domain
     * @return the update count
     */
    private int tryUpdate(Schema schema, Domain domain) {
        if (constraintName != null && schema.findConstraint(session, constraintName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraintName);
        }
        Database db = getDatabase();
        db.lockMeta(session);

        int id = getObjectId();
        String name = generateConstraintName(domain);
        ConstraintDomain constraint = new ConstraintDomain(schema, id, name, domain);
        constraint.setExpression(session, checkExpression);
        if (checkExisting) {
            constraint.checkExistingData(session);
        }
        constraint.setComment(comment);
        db.addSchemaObject(session, constraint);
        domain.addConstraint(constraint);
        return 0;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public String getConstraintName() {
        return constraintName;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_ADD_CONSTRAINT;
    }

    public void setCheckExpression(Expression expression) {
        this.checkExpression = expression;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

}
