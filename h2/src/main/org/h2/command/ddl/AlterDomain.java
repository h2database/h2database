/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.function.BiPredicate;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statements
 * ALTER DOMAIN SET DEFAULT
 * ALTER DOMAIN DROP DEFAULT
 * ALTER DOMAIN SET ON UPDATE
 * ALTER DOMAIN DROP ON UPDATE
 */
public class AlterDomain extends SchemaCommand {

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
    public static void forAllDependencies(Session session, Domain domain, BiPredicate<Domain, Column> columnProcessor,
            BiPredicate<Domain, Domain> domainProcessor, boolean recompileExpressions) {
        Database db = session.getDatabase();
        for (SchemaObject obj : db.getAllSchemaObjects(DbObject.DOMAIN)) {
            Domain targetDomain = (Domain) obj;
            if (targetDomain.getColumn().getDomain() == domain) {
                if (domainProcessor.test(domain, targetDomain)) {
                    if (recompileExpressions) {
                        domain.getColumn().prepareExpression(session);
                    }
                    db.updateMeta(session, targetDomain);
                }
            }
        }
        for (Table t : db.getAllTablesAndViews(false)) {
            boolean modified = false;
            for (Column targetColumn : t.getColumns()) {
                if (targetColumn.getDomain() == domain) {
                    boolean m = columnProcessor.test(domain, targetColumn);
                    if (m) {
                        if (recompileExpressions) {
                            targetColumn.prepareExpression(session);
                        }
                        modified = true;
                    }
                }
            }
            if (modified) {
                db.updateMeta(session, t);
            }
        }
    }

    private final int type;

    private Expression expression;

    private String domainName;
    private boolean ifDomainExists;

    public AlterDomain(Session session, Schema schema, int type) {
        super(session, schema);
        this.type = type;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public void setIfDomainExists(boolean b) {
        ifDomainExists = b;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Domain domain = getSchema().findDomain(domainName);
        if (domain == null) {
            if (ifDomainExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, domainName);
        }
        switch (type) {
        case CommandInterface.ALTER_DOMAIN_DEFAULT:
            domain.getColumn().setDefaultExpression(session, expression);
            break;
        case CommandInterface.ALTER_DOMAIN_ON_UPDATE:
            domain.getColumn().setOnUpdateExpression(session, expression);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        if (expression != null) {
            AlterDomain.forAllDependencies(session, domain, this::copyColumn, this::copyDomain, true);
        }
        session.getDatabase().updateMeta(session, domain);
        return 0;
    }

    private boolean copyColumn(Domain domain, Column targetColumn) {
        return copyExpressions(session, domain.getColumn(), targetColumn);
    }

    private boolean copyDomain(Domain domain, Domain targetDomain) {
        return copyExpressions(session, domain.getColumn(), targetDomain.getColumn());
    }

    private boolean copyExpressions(Session session, Column domainColumn, Column targetColumn) {
        switch (type) {
        case CommandInterface.ALTER_DOMAIN_DEFAULT: {
            Expression e = domainColumn.getDefaultExpression();
            if (e != null && targetColumn.getDefaultExpression() == null) {
                targetColumn.setDefaultExpression(session, e);
                return true;
            }
            break;
        }
        case CommandInterface.ALTER_DOMAIN_ON_UPDATE: {
            Expression e = domainColumn.getOnUpdateExpression();
            if (e != null && targetColumn.getOnUpdateExpression() == null) {
                targetColumn.setOnUpdateExpression(session, e);
                return true;
            }
        }
        }
        return false;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_DOMAIN_DROP_CONSTRAINT;
    }

}
