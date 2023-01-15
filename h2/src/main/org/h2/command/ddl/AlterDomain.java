/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.function.BiPredicate;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * The base class for ALTER DOMAIN commands.
 */
public abstract class AlterDomain extends SchemaOwnerCommand {

    /**
     * Processes all columns and domains that use the specified domain.
     *
     * @param session
     *            the session
     * @param domain
     *            the domain to process
     * @param columnProcessor
     *            column handler
     * @param domainProcessor
     *            domain handler
     * @param recompileExpressions
     *            whether processed expressions need to be recompiled
     */
    public static void forAllDependencies(SessionLocal session, Domain domain,
            BiPredicate<Domain, Column> columnProcessor, BiPredicate<Domain, Domain> domainProcessor,
            boolean recompileExpressions) {
        Database db = session.getDatabase();
        for (Schema schema : db.getAllSchemasNoMeta()) {
            for (Domain targetDomain : schema.getAllDomains()) {
                if (targetDomain.getDomain() == domain) {
                    if (domainProcessor == null || domainProcessor.test(domain, targetDomain)) {
                        if (recompileExpressions) {
                            domain.prepareExpressions(session);
                        }
                        db.updateMeta(session, targetDomain);
                    }
                }
            }
            for (Table t : schema.getAllTablesAndViews(null)) {
                if (forTable(session, domain, columnProcessor, recompileExpressions, t)) {
                    db.updateMeta(session, t);
                }
            }
        }
        for (Table t : session.getLocalTempTables()) {
            forTable(session, domain, columnProcessor, recompileExpressions, t);
        }
    }

    private static boolean forTable(SessionLocal session, Domain domain, BiPredicate<Domain, Column> columnProcessor,
            boolean recompileExpressions, Table t) {
        boolean modified = false;
        for (Column targetColumn : t.getColumns()) {
            if (targetColumn.getDomain() == domain) {
                boolean m = columnProcessor == null || columnProcessor.test(domain, targetColumn);
                if (m) {
                    if (recompileExpressions) {
                        targetColumn.prepareExpressions(session);
                    }
                    modified = true;
                }
            }
        }
        return modified;
    }

    String domainName;

    boolean ifDomainExists;

    AlterDomain(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public final void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public final void setIfDomainExists(boolean b) {
        ifDomainExists = b;
    }

    @Override
    final long update(Schema schema) {
        Domain domain = getSchema().findDomain(domainName);
        if (domain == null) {
            if (ifDomainExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, domainName);
        }
        return update(schema, domain);
    }

    abstract long update(Schema schema, Domain domain);

}
