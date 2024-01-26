/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER DOMAIN RENAME
 */
public class AlterDomainRename extends AlterDomain {

    private String newDomainName;

    public AlterDomainRename(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setNewDomainName(String name) {
        newDomainName = name;
    }

    @Override
    long update(Schema schema, Domain domain) {
        Domain d = schema.findDomain(newDomainName);
        if (d != null) {
            if (domain != d) {
                throw DbException.get(ErrorCode.DOMAIN_ALREADY_EXISTS_1, newDomainName);
            }
            if (newDomainName.equals(domain.getName())) {
                return 0;
            }
        }
        getDatabase().renameSchemaObject(session, domain, newDomainName);
        forAllDependencies(session, domain, null, null, false);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_RENAME;
    }

}
