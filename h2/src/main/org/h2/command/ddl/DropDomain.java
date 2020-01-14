/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintDomain;
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
 * This class represents the statement DROP DOMAIN
 */
public class DropDomain extends SchemaCommand {

    private String typeName;
    private boolean ifExists;
    private ConstraintActionType dropAction;

    public DropDomain(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintActionType.RESTRICT
                : ConstraintActionType.CASCADE;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Schema schema = getSchema();
        Domain domain = schema.findDomain(typeName);
        if (domain == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.DOMAIN_NOT_FOUND_1, typeName);
            }
        } else {
            Column domainColumn = domain.getColumn();
            for (SchemaObject obj : db.getAllSchemaObjects(DbObject.DOMAIN)) {
                Domain d = (Domain) obj;
                Column c = d.getColumn();
                if (c.getDomain() == domain) {
                    if (dropAction == ConstraintActionType.RESTRICT) {
                        throw DbException.get(ErrorCode.CANNOT_DROP_2, typeName, d.getTraceSQL());
                    }
                    ArrayList<ConstraintDomain> constraints = domain.getConstraints();
                    if (constraints != null && !constraints.isEmpty()) {
                        for (ConstraintDomain constraint : constraints) {
                            Expression checkCondition = constraint.getCheckConstraint(session, null);
                            AlterDomainAddConstraint check = new AlterDomainAddConstraint(session, d.getSchema(),
                                    false);
                            check.setDomainName(d.getName());
                            check.setCheckExpression(checkCondition);
                            check.update();
                        }
                    }
                    c.setOriginalSQL(domain.getColumn().getOriginalSQL());
                    c.setDomain(domainColumn.getDomain());
                    db.updateMeta(session, d);
                }
            }
            for (Table t : db.getAllTablesAndViews(false)) {
                boolean modified = false;
                for (Column c : t.getColumns()) {
                    if (c.getDomain() == domain) {
                        if (dropAction == ConstraintActionType.RESTRICT) {
                            throw DbException.get(ErrorCode.CANNOT_DROP_2, typeName, t.getCreateSQL());
                        }
                        String columnName = c.getName();
                        ArrayList<ConstraintDomain> constraints = domain.getConstraints();
                        if (constraints != null && !constraints.isEmpty()) {
                            for (ConstraintDomain constraint : constraints) {
                                Expression checkCondition = constraint.getCheckConstraint(session, columnName);
                                AlterTableAddConstraint check = new AlterTableAddConstraint(session, t.getSchema(),
                                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK, false);
                                check.setTableName(t.getName());
                                check.setCheckExpression(checkCondition);
                                check.update();
                            }
                        }
                        c.setOriginalSQL(domain.getColumn().getOriginalSQL());
                        c.setDomain(domainColumn.getDomain());
                        modified = true;
                    }
                }
                if (modified) {
                    db.updateMeta(session, t);
                }
            }
            session.getDatabase().removeSchemaObject(session, domain);
        }
        return 0;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
