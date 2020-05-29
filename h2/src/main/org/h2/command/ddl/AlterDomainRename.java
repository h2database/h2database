/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.util.HasSQL;

/**
 * This class represents the statement
 * ALTER DOMAIN RENAME
 */
public class AlterDomainRename extends SchemaCommand {

    private boolean ifDomainExists;
    private String oldDomainName;
    private String newDomainName;

    public AlterDomainRename(Session session, Schema schema) {
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
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Domain oldDomain = getSchema().findDomain(oldDomainName);
        if (oldDomain == null) {
            if (ifDomainExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, oldDomainName);
        }
        Domain d = getSchema().findDomain(newDomainName);
        if (d != null) {
            if (oldDomain != d) {
                throw DbException.get(ErrorCode.DOMAIN_ALREADY_EXISTS_1, newDomainName);
            }
            if (newDomainName.equals(oldDomain.getName())) {
                return 0;
            }
        }
        db.renameSchemaObject(session, oldDomain, newDomainName);
        AlterDomain.copy(session, oldDomain, this::copyColumn, this::copyDomain, false);
        return 0;
    }

    private boolean copyColumn(Domain domain, Column targetColumn) {
        updateOriginalSQL(session, domain, targetColumn);
        return true;
    }

    private boolean copyDomain(Domain domain, Domain targetDomain) {
        updateOriginalSQL(session, domain, targetDomain.getColumn());
        return true;
    }

    private static void updateOriginalSQL(Session session, Domain domain, Column targetColumn) {
        targetColumn.setOriginalSQL(domain.getSQL(HasSQL.DEFAULT_SQL_FLAGS));
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_RENAME;
    }

}
