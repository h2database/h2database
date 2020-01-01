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
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;

/**
 * This class represents the statement ALTER DOMAIN DROP CONSTRAINT
 */
public class AlterDomainDropConstraint extends SchemaCommand {

    private String constraintName;
    private String domainName;
    private boolean ifDomainExists;
    private final boolean ifConstraintExists;

    public AlterDomainDropConstraint(Session session, Schema schema, boolean ifConstraintExists) {
        super(session, schema);
        this.ifConstraintExists = ifConstraintExists;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public void setIfDomainExists(boolean b) {
        ifDomainExists = b;
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }

    @Override
    public int update() {
        session.commit(true);
        Domain domain = getSchema().findDomain(domainName);
        if (domain == null) {
            if (ifDomainExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, domainName);
        }
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        if (constraint == null || constraint.getConstraintType() != Type.DOMAIN
                || ((ConstraintDomain) constraint).getDomain() != domain) {
            if (!ifConstraintExists) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
            }
        } else {
            session.getUser().checkAdmin();
            session.getDatabase().removeSchemaObject(session, constraint);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_DROP_CONSTRAINT;
    }

}
