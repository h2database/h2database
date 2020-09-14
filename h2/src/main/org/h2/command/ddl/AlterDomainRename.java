/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER DOMAIN RENAME
 */
public class AlterDomainRename extends SchemaOwnerCommand {

    private boolean ifDomainExists;
    private String oldDomainName;
    private String newDomainName;

    public AlterDomainRename(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setIfDomainExists(boolean b) {
        ifDomainExists = b;
    }

    public void setOldDomainName(String name) {
        oldDomainName = name;
    }

    public void setNewDomainName(String name) {
        newDomainName = name;
    }

    @Override
    long update(Schema schema) {
        Database db = session.getDatabase();
        Domain oldDomain = schema.findDomain(oldDomainName);
        if (oldDomain == null) {
            if (ifDomainExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, oldDomainName);
        }
        Domain d = schema.findDomain(newDomainName);
        if (d != null) {
            if (oldDomain != d) {
                throw DbException.get(ErrorCode.DOMAIN_ALREADY_EXISTS_1, newDomainName);
            }
            if (newDomainName.equals(oldDomain.getName())) {
                return 0;
            }
        }
        db.renameSchemaObject(session, oldDomain, newDomainName);
        AlterDomain.forAllDependencies(session, oldDomain, null, null, false);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_RENAME;
    }

}
